/* Copyright (c) 2026-2026 Maik Mursall
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
#include <mgba/internal/gba/sio/netplay-lockstep.h>

#include <mgba/internal/gba/gba.h>
#include <mgba/internal/gba/io.h>

#define DRIVER_ID 0x4E506C73
/*
 * Keep the periodic event cadence moderate; transfer-critical wakeups come
 * from packet handling and explicit timing nudges.
 */
#define EVENT_IDLE_INTERVAL 8192
#define EVENT_ACTIVE_INTERVAL 8192
#define EVENT_BUSY_INTERVAL 1
#define EVENT_WAIT_INTERVAL 512
#define MAX_PACKET_SIZE 512
#define CONNECT_ID_WAIT_MS 5000
#define READER_IDLE_WAIT_MS 1

#ifndef NETPLAY_VERBOSE_TRANSFER_TRACE
#define NETPLAY_VERBOSE_TRANSFER_TRACE 0
#endif
#if NETPLAY_VERBOSE_TRANSFER_TRACE
#define NETPLAY_TRANSFER_TRACE(...) mLOG(GBA_SIO, DEBUG, __VA_ARGS__)
#else
#define NETPLAY_TRANSFER_TRACE(...) ((void) 0)
#endif
/* Force a timing re-baseline periodically to limit long-session drift. */
#define NETPLAY_CYCLE_RESYNC_INTERVAL 256
/*
 * If BEGIN target mapping drifts too far from local emulated time, resync
 * immediately instead of waiting for the periodic interval.
 */
#define NETPLAY_CYCLE_RESYNC_DRIFT_THRESHOLD (64 * EVENT_ACTIVE_INTERVAL)
/* Grace window for modes that may reuse current register values if no fresh write arrives quickly. */
#define NETPLAY_SAMPLE_FRESH_REUSE_WAIT_CYCLES 2048
/*
 * MULTI is primarily fresh-write driven, but some game states legitimately keep
 * SIOMLT_SEND unchanged across consecutive transfers. Allow same-generation
 * fallback after a short wait to avoid transfer deadlock.
 */
#define NETPLAY_MULTI_SAME_GENERATION_FALLBACK_WAIT_CYCLES 2048
/*
 * If BEGIN handling is already late, don't wait the full fallback window; give
 * a short grace period for a just-imminent SIOMLT_SEND write first.
 */
#define NETPLAY_MULTI_LATE_FRESH_GRACE_CYCLES (2 * EVENT_ACTIVE_INTERVAL)
/*
 * Deferred MULTI completion poll cadence for netplay.
 * Keep wait polls coarse to avoid emu-thread churn while RESULT is in flight.
 * RESULT-ready path still wakes at near-immediate cadence.
 */
#define NETPLAY_MULTI_FINISH_POLL_WAIT_CYCLES 8192
#define NETPLAY_MULTI_FINISH_POLL_READY_CYCLES 1
/*
 * Host keeps blocking MULTI completion semantics. Use a conservative timeout so
 * true hangs can recover without regressing normal high-latency sessions.
 */
#define NETPLAY_HOST_MULTI_FINISH_TIMEOUT_MS 5000
#define NETPLAY_HOST_MULTI_FINISH_WAIT_STEP_MS 2

#define MSG_HELLO 0x01
#define MSG_MODE 0x02
#define MSG_TRANSFER_START 0x03
#define MSG_TRANSFER_DATA 0x04

#define MSG_STATE 0x10
#define MSG_TRANSFER_BEGIN 0x11
#define MSG_TRANSFER_RESULT 0x12

const uint16_t GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT = 7777;
const char GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST[] = "127.0.0.1";

struct NetPlayTransferSample {
	uint16_t siocnt;
	uint16_t send16;
	uint32_t send32;
};

static bool GBASIONetPlayLockstepDriverInit(struct GBASIODriver* driver);
static void GBASIONetPlayLockstepDriverDeinit(struct GBASIODriver* driver);
static void GBASIONetPlayLockstepDriverReset(struct GBASIODriver* driver);
static uint32_t GBASIONetPlayLockstepDriverId(const struct GBASIODriver* driver);
static void GBASIONetPlayLockstepDriverSetMode(struct GBASIODriver* driver, enum GBASIOMode mode);
static bool GBASIONetPlayLockstepDriverHandlesMode(struct GBASIODriver* driver, enum GBASIOMode mode);
static int GBASIONetPlayLockstepDriverConnectedDevices(struct GBASIODriver* driver);
static int GBASIONetPlayLockstepDriverDeviceId(struct GBASIODriver* driver);
static uint16_t GBASIONetPlayLockstepDriverWriteSIOCNT(struct GBASIODriver* driver, uint16_t value);
static uint16_t GBASIONetPlayLockstepDriverWriteRCNT(struct GBASIODriver* driver, uint16_t value);
static uint16_t GBASIONetPlayLockstepDriverWriteRegister(struct GBASIODriver* driver, uint32_t address, uint16_t value);
static bool GBASIONetPlayLockstepDriverStart(struct GBASIODriver* driver);
static void GBASIONetPlayLockstepDriverFinishMultiplayer(struct GBASIODriver* driver, uint16_t data[4]);
static bool GBASIONetPlayLockstepDriverFinishMultiplayerPoll(struct GBASIODriver* driver, uint16_t data[4]);
static uint32_t GBASIONetPlayLockstepDriverFinishMultiplayerPollInterval(struct GBASIODriver* driver);
static uint8_t GBASIONetPlayLockstepDriverFinishNormal8(struct GBASIODriver* driver);
static uint32_t GBASIONetPlayLockstepDriverFinishNormal32(struct GBASIODriver* driver);

static void _netPlayEvent(struct mTiming* timing, void* context, uint32_t cyclesLate);

static bool _sendPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size);
static void _setDisconnected(struct GBASIONetPlayLockstepDriver* driver, bool remoteClose);
static void _updateReadyState(struct GBASIONetPlayLockstepDriver* driver);
static void _clearFreshnessWait(struct GBASIONetPlayLockstepDriver* driver);
static uint32_t _sampleWriteGenerationForMode(const struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode);
static uint32_t _sampleLastSentGenerationForMode(const struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode);
static void _setSampleLastSentGenerationForMode(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, uint32_t generation);
static void _recordMultiSendWriteSample(struct GBASIONetPlayLockstepDriver* driver, uint16_t value, uint32_t generation, int32_t cycle, bool hasCycle);
static void _syncSIOCNTFromBegin(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, uint16_t beginSIOCNT);
static void _logTransferControlSnapshot(struct GBASIONetPlayLockstepDriver* driver, const char* phase, uint32_t sequence, enum GBASIOMode mode, uint16_t packetSIOCNT);
static bool _captureTransferSample(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct NetPlayTransferSample* sample);
static bool _sendTransferSample(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, uint32_t sequence, enum GBASIOMode mode, const struct NetPlayTransferSample* sample, int32_t startCycle, bool hasStartCycle);
static bool _waitForTransferResult(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct GBASIONetPlayLockstepTransferResult* out, int32_t timeoutMs);
static bool _pollMultiplayerSecondaryFinish(struct GBASIONetPlayLockstepDriver* driver, uint16_t data[4]);
static void _rescheduleDriverEvent(struct mTiming* timing, struct GBASIONetPlayLockstepDriver* driver, uint32_t when);
static struct mLockstepUser* _wakeDriverLocked(struct GBASIONetPlayLockstepDriver* driver);
static void _wakeDriver(struct GBASIONetPlayLockstepDriver* driver);
static bool _tryGetLocalCycle(struct GBASIONetPlayLockstepDriver* driver, int32_t* outCycle);

#ifndef DISABLE_THREADING
static THREAD_ENTRY _readerThread(void* context);
static bool _recvAll(Socket socket, void* out, size_t size);
static bool _handleIncomingPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size);
#endif

static uint16_t _read16BE(const uint8_t* data) {
	return data[0] << 8 | data[1];
}

static uint32_t _read32BE(const uint8_t* data) {
	return (uint32_t) data[0] << 24 | (uint32_t) data[1] << 16 | (uint32_t) data[2] << 8 | data[3];
}

static void _write16BE(uint8_t* out, uint16_t value) {
	out[0] = value >> 8;
	out[1] = value;
}

static void _write32BE(uint8_t* out, uint32_t value) {
	out[0] = value >> 24;
	out[1] = value >> 16;
	out[2] = value >> 8;
	out[3] = value;
}

static uint8_t _modeToWire(enum GBASIOMode mode) {
	switch (mode) {
	case GBA_SIO_NORMAL_8:
	case GBA_SIO_NORMAL_32:
	case GBA_SIO_MULTI:
	case GBA_SIO_UART:
	case GBA_SIO_GPIO:
	case GBA_SIO_JOYBUS:
		return (uint8_t) mode;
	default:
		return 0xFF;
	}
}

static enum GBASIOMode _modeFromWire(uint8_t mode) {
	switch (mode) {
	case GBA_SIO_NORMAL_8:
	case GBA_SIO_NORMAL_32:
	case GBA_SIO_MULTI:
	case GBA_SIO_UART:
	case GBA_SIO_GPIO:
	case GBA_SIO_JOYBUS:
		return (enum GBASIOMode) mode;
	default:
		return (enum GBASIOMode) -1;
	}
}

static bool _pendingBeginQueueContainsSequence(const struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence) {
	if (!driver->pendingBeginCount) {
		return false;
	}
	return driver->pendingBegins[driver->pendingBeginRead].sequence == sequence;
}

static bool _pendingBeginQueuePush(struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence, enum GBASIOMode mode, uint8_t attached, uint16_t siocnt, int32_t startCycle, bool hasStartCycle) {
	struct GBASIONetPlayLockstepPendingBegin* begin;
	if (driver->pendingBeginCount) {
		return false;
	}
	begin = &driver->pendingBegins[0];
	begin->sequence = sequence;
	begin->mode = mode;
	begin->attached = attached;
	begin->siocnt = siocnt;
	begin->startCycle = startCycle;
	begin->hasStartCycle = hasStartCycle;
	driver->pendingBeginRead = 0;
	driver->pendingBeginWrite = 1 % NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE;
	driver->pendingBeginCount = 1;
	return true;
}

static bool _pendingBeginQueuePeek(const struct GBASIONetPlayLockstepDriver* driver, struct GBASIONetPlayLockstepPendingBegin* out) {
	if (!driver->pendingBeginCount) {
		return false;
	}
	*out = driver->pendingBegins[driver->pendingBeginRead];
	return true;
}

static void _pendingBeginQueuePop(struct GBASIONetPlayLockstepDriver* driver) {
	if (!driver->pendingBeginCount) {
		return;
	}
	driver->pendingBeginRead = 0;
	driver->pendingBeginWrite = 0;
	driver->pendingBeginCount = 0;
}

static bool _pendingResultQueueContainsSequence(const struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence) {
	if (!driver->pendingResultCount) {
		return false;
	}
	return driver->pendingResults[driver->pendingResultRead].sequence == sequence;
}

static bool _pendingResultQueuePush(struct GBASIONetPlayLockstepDriver* driver, const struct GBASIONetPlayLockstepTransferResult* result) {
	if (_pendingResultQueueContainsSequence(driver, result->sequence)) {
		return true;
	}
	if (driver->pendingResultCount) {
		return false;
	}
	driver->pendingResults[0] = *result;
	driver->pendingResultRead = 0;
	driver->pendingResultWrite = 1 % NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE;
	driver->pendingResultCount = 1;
	return true;
}

static bool _pendingResultQueuePopSequence(struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence, struct GBASIONetPlayLockstepTransferResult* out) {
	if (!driver->pendingResultCount) {
		return false;
	}
	if (driver->pendingResults[driver->pendingResultRead].sequence != sequence) {
		return false;
	}
	if (out) {
		*out = driver->pendingResults[driver->pendingResultRead];
	}
	driver->pendingResultRead = 0;
	driver->pendingResultWrite = 0;
	driver->pendingResultCount = 0;
	return true;
}

static bool _hasPendingResultForTransfer(const struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence) {
	if (!sequence) {
		return false;
	}
	return _pendingResultQueueContainsSequence(driver, sequence);
}

void GBASIONetPlayLockstepDriverCreate(struct GBASIONetPlayLockstepDriver* driver, struct mLockstepUser* user) {
	memset(driver, 0, sizeof(*driver));
	driver->d.init = GBASIONetPlayLockstepDriverInit;
	driver->d.deinit = GBASIONetPlayLockstepDriverDeinit;
	driver->d.reset = GBASIONetPlayLockstepDriverReset;
	driver->d.driverId = GBASIONetPlayLockstepDriverId;
	driver->d.setMode = GBASIONetPlayLockstepDriverSetMode;
	driver->d.handlesMode = GBASIONetPlayLockstepDriverHandlesMode;
	driver->d.connectedDevices = GBASIONetPlayLockstepDriverConnectedDevices;
	driver->d.deviceId = GBASIONetPlayLockstepDriverDeviceId;
	driver->d.writeSIOCNT = GBASIONetPlayLockstepDriverWriteSIOCNT;
	driver->d.writeRCNT = GBASIONetPlayLockstepDriverWriteRCNT;
	driver->d.writeRegister = GBASIONetPlayLockstepDriverWriteRegister;
	driver->d.start = GBASIONetPlayLockstepDriverStart;
	driver->d.finishMultiplayer = GBASIONetPlayLockstepDriverFinishMultiplayer;
	driver->d.finishMultiplayerPoll = GBASIONetPlayLockstepDriverFinishMultiplayerPoll;
	driver->d.finishMultiplayerPollInterval = GBASIONetPlayLockstepDriverFinishMultiplayerPollInterval;
	driver->d.finishNormal8 = GBASIONetPlayLockstepDriverFinishNormal8;
	driver->d.finishNormal32 = GBASIONetPlayLockstepDriverFinishNormal32;

	driver->event.context = driver;
	driver->event.callback = _netPlayEvent;
	driver->event.name = "GBA SIO NetPlay Lockstep";
	driver->event.priority = 0x80;

	driver->socket = INVALID_SOCKET;
	driver->playerId = -1;
	driver->attached = 1;
	driver->mode = (enum GBASIOMode) -1;
	driver->user = user;
#ifndef DISABLE_THREADING
	MutexInit(&driver->mutex);
	MutexInit(&driver->sendMutex);
	ConditionInit(&driver->cond);
	driver->readerLogger = NULL;
#endif
}

void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver* driver) {
	_setDisconnected(driver, false);
#ifndef DISABLE_THREADING
	ConditionDeinit(&driver->cond);
	MutexDeinit(&driver->sendMutex);
	MutexDeinit(&driver->mutex);
#endif
}

static bool GBASIONetPlayLockstepDriverInit(struct GBASIODriver* driver) {
	GBASIONetPlayLockstepDriverReset(driver);
	return true;
}

static void GBASIONetPlayLockstepDriverDeinit(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	mTimingDeschedule(&driver->p->p->timing, &net->event);
	_setDisconnected(net, false);
}

static void GBASIONetPlayLockstepDriverReset(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	net->pendingBeginRead = 0;
	net->pendingBeginWrite = 0;
	net->pendingBeginCount = 0;
	net->pendingResultRead = 0;
	net->pendingResultWrite = 0;
	net->pendingResultCount = 0;
	net->stateDirty = true;
	net->waitingForTransfer = false;
	net->transferActive = false;
	net->deferredMultiplayerResultValid = false;
	net->cycleSyncValid = false;
	net->cycleSyncOffset = 0;
	net->cycleSyncSequence = 0;
	net->multiSendWriteGeneration = 0;
	net->normal8WriteGeneration = 0;
	net->normal32WriteGeneration = 0;
	net->multiSendWriteHistoryWrite = 0;
	net->multiSendWriteHistoryCount = 0;
	net->multiSendLastSentGeneration = UINT32_MAX;
	net->normal8LastSentGeneration = UINT32_MAX;
	net->normal32LastSentGeneration = UINT32_MAX;
	_clearFreshnessWait(net);
	net->multiSameGenerationFastPathActive = false;
	net->multiSameGenerationFastPathGeneration = 0;
	net->asleep = false;
	net->clientIdleEvents = 0;
	net->mode = driver->p ? driver->p->mode : (enum GBASIOMode) -1;
	if (net->playerId >= 0 && net->playerId < MAX_GBAS) {
		net->otherModes[net->playerId] = net->mode;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif
	if (driver->p && driver->p->p) {
		mTimingDeschedule(&driver->p->p->timing, &net->event);
		mTimingSchedule(&driver->p->p->timing, &net->event, 0);
	}
}

static uint32_t GBASIONetPlayLockstepDriverId(const struct GBASIODriver* driver) {
	UNUSED(driver);
	return DRIVER_ID;
}

static bool _supportsTransferMode(enum GBASIOMode mode) {
	return mode == GBA_SIO_MULTI || mode == GBA_SIO_NORMAL_8 || mode == GBA_SIO_NORMAL_32;
}

static bool _requiresStrictFreshSample(enum GBASIOMode mode) {
	/*
	 * MULTI/NORMAL8 are write-driven and should publish a fresh local sample.
	 * NORMAL32 commonly reuses SIODATA32 across consecutive transfers.
	 */
	return mode == GBA_SIO_MULTI || mode == GBA_SIO_NORMAL_8;
}

static void _clearFreshnessWait(struct GBASIONetPlayLockstepDriver* driver) {
	driver->freshnessWaitActive = false;
	driver->freshnessWaitLogged = false;
	driver->freshnessWaitSequence = 0;
	driver->freshnessWaitMode = (enum GBASIOMode) -1;
	driver->freshnessWaitBaselineGeneration = 0;
	driver->freshnessWaitStartCycle = 0;
}

static uint32_t _sampleWriteGenerationForMode(const struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode) {
	switch (mode) {
	case GBA_SIO_MULTI:
		return driver->multiSendWriteGeneration;
	case GBA_SIO_NORMAL_8:
		return driver->normal8WriteGeneration;
	case GBA_SIO_NORMAL_32:
		return driver->normal32WriteGeneration;
	default:
		return 0;
	}
}

static uint32_t _sampleLastSentGenerationForMode(const struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode) {
	switch (mode) {
	case GBA_SIO_MULTI:
		return driver->multiSendLastSentGeneration;
	case GBA_SIO_NORMAL_8:
		return driver->normal8LastSentGeneration;
	case GBA_SIO_NORMAL_32:
		return driver->normal32LastSentGeneration;
	default:
		return 0;
	}
}

static void _setSampleLastSentGenerationForMode(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, uint32_t generation) {
	switch (mode) {
	case GBA_SIO_MULTI:
		driver->multiSendLastSentGeneration = generation;
		break;
	case GBA_SIO_NORMAL_8:
		driver->normal8LastSentGeneration = generation;
		break;
	case GBA_SIO_NORMAL_32:
		driver->normal32LastSentGeneration = generation;
		break;
	default:
		break;
	}
}

static bool _isSampleGenerationNewer(uint32_t generation, uint32_t baselineGeneration) {
	return (int32_t) (generation - baselineGeneration) > 0;
}

static void _recordMultiSendWriteSample(struct GBASIONetPlayLockstepDriver* driver, uint16_t value, uint32_t generation, int32_t cycle, bool hasCycle) {
	uint8_t idx;
	if (!hasCycle) {
		return;
	}
	idx = driver->multiSendWriteHistoryWrite;
	driver->multiSendWriteHistoryValue[idx] = value;
	driver->multiSendWriteHistoryCycle[idx] = cycle;
	driver->multiSendWriteHistoryGeneration[idx] = generation;
	driver->multiSendWriteHistoryWrite = (driver->multiSendWriteHistoryWrite + 1) % NETPLAY_LOCKSTEP_MULTI_WRITE_HISTORY_SIZE;
	if (driver->multiSendWriteHistoryCount < NETPLAY_LOCKSTEP_MULTI_WRITE_HISTORY_SIZE) {
		++driver->multiSendWriteHistoryCount;
	}
}

static void GBASIONetPlayLockstepDriverSetMode(struct GBASIODriver* driver, enum GBASIOMode mode) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	bool connected;
	int playerId;
	uint8_t payload[1];
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	if (mode == net->mode) {
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		return;
	}
	net->mode = mode;
	playerId = net->playerId;
	if (playerId >= 0 && playerId < MAX_GBAS) {
		net->otherModes[playerId] = mode;
	}
	connected = net->connected;
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif

	_updateReadyState(net);
	if (connected) {
		payload[0] = _modeToWire(mode);
		if (!_sendPacket(net, MSG_MODE, payload, sizeof(payload))) {
			_setDisconnected(net, true);
		}
	}
}

static bool GBASIONetPlayLockstepDriverHandlesMode(struct GBASIODriver* driver, enum GBASIOMode mode) {
	UNUSED(driver);
	UNUSED(mode);
	return true;
}

static int GBASIONetPlayLockstepDriverConnectedDevices(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	int attached;
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	attached = net->connected ? net->attached : 1;
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif
	if (attached < 1) {
		return 0;
	}
	return attached - 1;
}

static int GBASIONetPlayLockstepDriverDeviceId(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	int playerId;
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	playerId = net->playerId;
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif
	if (playerId < 0 || playerId >= MAX_GBAS) {
		return 0;
	}
	return playerId;
}

static uint16_t GBASIONetPlayLockstepDriverWriteSIOCNT(struct GBASIODriver* driver, uint16_t value) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: SIOCNT <- %04X", value);
	_updateReadyState(net);
	return value;
}

static uint16_t GBASIONetPlayLockstepDriverWriteRCNT(struct GBASIODriver* driver, uint16_t value) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: RCNT <- %04X", value);
	_updateReadyState(net);
	return value;
}

static uint16_t GBASIONetPlayLockstepDriverWriteRegister(struct GBASIODriver* driver, uint32_t address, uint16_t value) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	bool nudgeFreshness = false;
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	switch (address) {
	case GBA_REG_SIOMLT_SEND:
	{
		int32_t writeCycle = 0;
		bool hasWriteCycle = false;
		if (net->d.p && net->d.p->p) {
			writeCycle = mTimingCurrentTime(&net->d.p->p->timing);
			hasWriteCycle = true;
		}
		++net->multiSendWriteGeneration;
		++net->normal8WriteGeneration;
		_recordMultiSendWriteSample(net, value, net->multiSendWriteGeneration, writeCycle, hasWriteCycle);
		/*
		 * A new SIOMLT_SEND write ends any prior same-generation plateau.
		 * Require at least one bounded wait again before re-enabling fast reuse.
		 */
		net->multiSameGenerationFastPathActive = false;
		net->multiSameGenerationFastPathGeneration = net->multiSendWriteGeneration;
		nudgeFreshness = net->freshnessWaitActive && _requiresStrictFreshSample(net->freshnessWaitMode);
		break;
	}
	case GBA_REG_SIODATA32_LO:
	case GBA_REG_SIODATA32_HI:
		++net->normal32WriteGeneration;
		nudgeFreshness = false;
		break;
	default:
		break;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif
	if (nudgeFreshness && net->d.p && net->d.p->p) {
		mTimingDeschedule(&net->d.p->p->timing, &net->event);
		mTimingSchedule(&net->d.p->p->timing, &net->event, 1);
	}
	return value;
}

static bool GBASIONetPlayLockstepDriverStart(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	struct NetPlayTransferSample sample;
	bool connected;
	bool transferInFlight;
	bool waitedForTransfer = false;
	bool transferActive;
	bool waitingForTransfer;
	bool hasPendingResult;
	int attached;
	int playerId;
	uint32_t currentSequence;
	uint32_t waitedMs = 0;
	uint32_t sequence;
	int32_t startCycle = 0;
	bool hasStartCycle = false;
	enum GBASIOMode mode = driver->p->mode;

	if (!_supportsTransferMode(mode)) {
		return true;
	}
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	connected = net->connected;
	attached = net->attached;
	playerId = net->playerId;
	transferActive = net->transferActive;
	waitingForTransfer = net->waitingForTransfer;
	currentSequence = net->transferSequence;
	hasPendingResult = _hasPendingResultForTransfer(net, currentSequence);
	transferInFlight = transferActive || waitingForTransfer || hasPendingResult;
	while (transferInFlight && connected && playerId == 0) {
		if (!waitedForTransfer) {
			mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: host START waiting for transfer %u to finish (active=%d waiting=%d pendingResult=%d)",
			     (unsigned) currentSequence, transferActive, waitingForTransfer, hasPendingResult);
		}
		waitedForTransfer = true;
#ifndef DISABLE_THREADING
		ConditionWaitTimed(&net->cond, &net->mutex, 1);
#else
		break;
#endif
		++waitedMs;
		connected = net->connected;
		attached = net->attached;
		playerId = net->playerId;
		transferActive = net->transferActive;
		waitingForTransfer = net->waitingForTransfer;
		currentSequence = net->transferSequence;
		hasPendingResult = _hasPendingResultForTransfer(net, currentSequence);
		transferInFlight = transferActive || waitingForTransfer || hasPendingResult;
	}
	if (!connected) {
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		mLOG(GBA_SIO, WARN, "Transfer requested while relay is disconnected");
		return false;
	}
	if (playerId != 0) {
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		/* Secondary transfers are begin-driven; START on non-primary is ignored. */
		mLOG(GBA_SIO, DEBUG, "Secondary player attempted to start transfer (ignored)");
		return false;
	}
	if (transferInFlight) {
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		mLOG(GBA_SIO, WARN, "Transfer still in flight after wait (active=%d waiting=%d pendingResult=%d seq=%u)",
		     transferActive, waitingForTransfer, hasPendingResult, (unsigned) currentSequence);
		return false;
	}
	if (waitedForTransfer) {
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: host START resumed after waiting %u ms", (unsigned) waitedMs);
	}
	if (attached < 2) {
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		mLOG(GBA_SIO, DEBUG, "Attempted to start transfer without remote player");
		return false;
	}
	++net->transferSequence;
	if (!net->transferSequence) {
		++net->transferSequence;
	}
	sequence = net->transferSequence;
	net->transferActive = true;
	net->waitingForTransfer = true;
	net->deferredMultiplayerResultValid = false;
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif

	if (!_captureTransferSample(net, mode, &sample)) {
#ifndef DISABLE_THREADING
		MutexLock(&net->mutex);
#endif
		net->waitingForTransfer = false;
		net->transferActive = false;
#ifndef DISABLE_THREADING
		ConditionWake(&net->cond);
#endif
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		return false;
	}
	hasStartCycle = _tryGetLocalCycle(net, &startCycle);

	if (!_sendTransferSample(net, MSG_TRANSFER_START, sequence, mode, &sample, startCycle, hasStartCycle)) {
#ifndef DISABLE_THREADING
		MutexLock(&net->mutex);
#endif
		net->waitingForTransfer = false;
		net->transferActive = false;
#ifndef DISABLE_THREADING
		ConditionWake(&net->cond);
#endif
#ifndef DISABLE_THREADING
		MutexUnlock(&net->mutex);
#endif
		_setDisconnected(net, true);
		return false;
	}
	return true;
}

bool GBASIONetPlayLockstepDriverConnect(struct GBASIONetPlayLockstepDriver* driver, const char* host, uint16_t port) {
#ifdef DISABLE_THREADING
	UNUSED(driver);
	UNUSED(host);
	UNUSED(port);
	mLOG(GBA_SIO, ERROR, "NetPlay lockstep requires threading support");
	return false;
#else
	struct Address address;
	Socket socket;
	uint8_t hello[4] = { 0xFF, 0, 0, 0 };
	int requestedId = -1;
	if (!host) {
		host = GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST;
	}
	if (!port) {
		port = GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT;
	}
	if (SocketResolveHost(host, &address)) {
		mLOG(GBA_SIO, ERROR, "Could not resolve host '%s'", host);
		return false;
	}
	socket = SocketConnectTCP(port, &address);
	if (SOCKET_FAILED(socket)) {
		mLOG(GBA_SIO, ERROR, "Could not connect to relay server %s:%u", host, port);
		return false;
	}
	SocketSetTCPPush(socket, true);

	_setDisconnected(driver, false);

	if (driver->user && driver->user->requestedId) {
		requestedId = driver->user->requestedId(driver->user);
	}
	if (requestedId >= 0 && requestedId < MAX_GBAS) {
		hello[0] = requestedId;
	}

	MutexLock(&driver->mutex);
	driver->socket = socket;
	driver->connected = true;
	driver->stopping = false;
	driver->pendingDisconnect = false;
	driver->playerId = -1;
	driver->attached = 1;
	driver->stateDirty = true;
	driver->waitingForTransfer = false;
	driver->transferActive = false;
	driver->deferredMultiplayerResultValid = false;
	driver->cycleSyncValid = false;
	driver->cycleSyncOffset = 0;
	driver->cycleSyncSequence = 0;
	driver->multiSendWriteGeneration = 0;
	driver->normal8WriteGeneration = 0;
	driver->normal32WriteGeneration = 0;
	driver->multiSendWriteHistoryWrite = 0;
	driver->multiSendWriteHistoryCount = 0;
	driver->multiSendLastSentGeneration = UINT32_MAX;
	driver->normal8LastSentGeneration = UINT32_MAX;
	driver->normal32LastSentGeneration = UINT32_MAX;
	_clearFreshnessWait(driver);
	driver->multiSameGenerationFastPathActive = false;
	driver->multiSameGenerationFastPathGeneration = 0;
	driver->asleep = false;
	driver->clientIdleEvents = 0;
	driver->pendingBeginRead = 0;
	driver->pendingBeginWrite = 0;
	driver->pendingBeginCount = 0;
	driver->pendingResultRead = 0;
	driver->pendingResultWrite = 0;
	driver->pendingResultCount = 0;
	driver->readerLogger = mLogGetContext();
	MutexUnlock(&driver->mutex);

	if (!_sendPacket(driver, MSG_HELLO, hello, sizeof(hello))) {
		_setDisconnected(driver, true);
		mLOG(GBA_SIO, ERROR, "Failed to send HELLO to relay server");
		return false;
	}

#ifdef _MSC_VER
	SetLastError(ERROR_SUCCESS);
#endif
	if (ThreadCreate(&driver->thread, _readerThread, driver)) {
		mLOG(GBA_SIO, ERROR, "Could not start relay reader thread");
		_setDisconnected(driver, false);
		return false;
	}
	MutexLock(&driver->mutex);
	driver->threadRunning = true;
	MutexUnlock(&driver->mutex);
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: relay reader thread started");

	if (driver->d.p) {
		mTimingInterrupt(&driver->d.p->p->timing);
	}

	{
		const int32_t waitStepMs = 50;
		int32_t waitedMs = 0;
		bool connected = false;
		int assignedPlayerId = -1;
		MutexLock(&driver->mutex);
		while (driver->connected && driver->playerId < 0 && waitedMs < CONNECT_ID_WAIT_MS) {
			ConditionWaitTimed(&driver->cond, &driver->mutex, waitStepMs);
			waitedMs += waitStepMs;
		}
		connected = driver->connected;
		assignedPlayerId = driver->playerId;
		MutexUnlock(&driver->mutex);

		if (!connected || assignedPlayerId < 0) {
			mLOG(GBA_SIO, ERROR, "Did not receive assigned player ID from relay within %d ms", CONNECT_ID_WAIT_MS);
			_setDisconnected(driver, false);
			return false;
		}
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: assigned player ID from relay: %d", assignedPlayerId);
	}

	return true;
#endif
}

bool GBASIONetPlayLockstepDriverConnectDefault(struct GBASIONetPlayLockstepDriver* driver) {
	return GBASIONetPlayLockstepDriverConnect(driver, GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST, GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT);
}

bool GBASIONetPlayLockstepDriverIsConnected(const struct GBASIONetPlayLockstepDriver* driver) {
	bool connected;
#ifndef DISABLE_THREADING
	MutexLock((Mutex*) &driver->mutex);
#endif
	connected = driver->connected;
#ifndef DISABLE_THREADING
	MutexUnlock((Mutex*) &driver->mutex);
#endif
	return connected;
}

static void _wakeDriver(struct GBASIONetPlayLockstepDriver* driver) {
	struct mLockstepUser* user = NULL;
#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	user = _wakeDriverLocked(driver);
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif
	if (user) {
		user->wake(user);
	}
}

static void _rescheduleDriverEvent(struct mTiming* timing, struct GBASIONetPlayLockstepDriver* driver, uint32_t when) {
	if (!when) {
		when = 1;
	}
	while (mTimingIsScheduled(timing, &driver->event)) {
		mTimingDeschedule(timing, &driver->event);
	}
	mTimingSchedule(timing, &driver->event, (int32_t) when);
}

static struct mLockstepUser* _wakeDriverLocked(struct GBASIONetPlayLockstepDriver* driver) {
	driver->clientIdleEvents = 0;
	if (driver->asleep && driver->user && driver->user->wake) {
		++driver->wakeGeneration;
		driver->asleep = false;
		return driver->user;
	}
	return NULL;
}

static bool _sendPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size) {
	uint8_t header[8] = { 0 };
	size_t sent;
	ssize_t written;
	Socket socket;
	bool ok = false;

	if (size > UINT32_MAX) {
		return false;
	}

#ifndef DISABLE_THREADING
	MutexLock(&driver->sendMutex);
#endif
	if (!driver->connected || SOCKET_FAILED(driver->socket)) {
#ifndef DISABLE_THREADING
		MutexUnlock(&driver->sendMutex);
#endif
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: send failed (disconnected) type=%u size=%u", type, (unsigned) size);
		return false;
	}
	socket = driver->socket;

	header[0] = type;
	_write32BE(&header[4], (uint32_t) size);
	sent = 0;
	while (sent < sizeof(header)) {
		written = SocketSend(socket, &header[sent], sizeof(header) - sent);
		if (written <= 0) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: send header failed type=%u", type);
			goto done;
		}
		sent += written;
	}

	sent = 0;
	while (sent < size) {
		written = SocketSend(socket, &payload[sent], size - sent);
		if (written <= 0) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: send payload failed type=%u", type);
			goto done;
		}
		sent += written;
	}
	ok = true;

done:
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->sendMutex);
#endif
	return ok;
}

static void _setDisconnected(struct GBASIONetPlayLockstepDriver* driver, bool remoteClose) {
#ifndef DISABLE_THREADING
	bool joinThread = false;
#endif
	Socket socket = INVALID_SOCKET;

#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	if (!driver->connected && SOCKET_FAILED(driver->socket)
#ifndef DISABLE_THREADING
		&& !driver->threadRunning
#endif
	) {
#ifndef DISABLE_THREADING
		MutexUnlock(&driver->mutex);
#endif
		return;
	}

	driver->connected = false;
	driver->waitingForTransfer = false;
	driver->transferActive = false;
	driver->deferredMultiplayerResultValid = false;
	driver->cycleSyncValid = false;
	driver->cycleSyncOffset = 0;
	driver->cycleSyncSequence = 0;
	driver->multiSendWriteGeneration = 0;
	driver->normal8WriteGeneration = 0;
	driver->normal32WriteGeneration = 0;
	driver->multiSendWriteHistoryWrite = 0;
	driver->multiSendWriteHistoryCount = 0;
	driver->multiSendLastSentGeneration = UINT32_MAX;
	driver->normal8LastSentGeneration = UINT32_MAX;
	driver->normal32LastSentGeneration = UINT32_MAX;
	_clearFreshnessWait(driver);
	driver->multiSameGenerationFastPathActive = false;
	driver->multiSameGenerationFastPathGeneration = 0;
	driver->asleep = false;
	driver->clientIdleEvents = 0;
	driver->pendingBeginRead = 0;
	driver->pendingBeginWrite = 0;
	driver->pendingBeginCount = 0;
	driver->pendingResultRead = 0;
	driver->pendingResultWrite = 0;
	driver->pendingResultCount = 0;
	driver->stateDirty = true;
	if (remoteClose) {
		driver->pendingDisconnect = true;
	}
#ifndef DISABLE_THREADING
	ConditionWake(&driver->cond);
	driver->stopping = true;
	joinThread = driver->threadRunning;
#endif
	socket = driver->socket;
	driver->socket = INVALID_SOCKET;
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif

	if (!SOCKET_FAILED(socket)) {
		SocketClose(socket);
	}
#ifndef DISABLE_THREADING
	if (joinThread) {
		ThreadJoin(&driver->thread);
		MutexLock(&driver->mutex);
		driver->threadRunning = false;
		MutexUnlock(&driver->mutex);
	}
#endif
	_wakeDriver(driver);
}

static bool _tryGetLocalCycle(struct GBASIONetPlayLockstepDriver* driver, int32_t* outCycle) {
	struct GBASIO* sio = driver->d.p;
	if (!sio || !sio->p) {
		return false;
	}
	*outCycle = mTimingCurrentTime(&sio->p->timing);
	return true;
}

static bool _captureTransferSample(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct NetPlayTransferSample* sample) {
	struct GBASIO* sio = driver->d.p;
	memset(sample, 0, sizeof(*sample));
	if (!sio) {
		return false;
	}
	sample->siocnt = sio->siocnt;
	switch (mode) {
	case GBA_SIO_MULTI:
		sample->send16 = sio->p->memory.io[GBA_REG(SIOMLT_SEND)];
		break;
	case GBA_SIO_NORMAL_8:
		sample->send32 = sio->p->memory.io[GBA_REG(SIODATA8)] & 0xFF;
		break;
	case GBA_SIO_NORMAL_32:
		sample->send32 = sio->p->memory.io[GBA_REG(SIODATA32_LO)];
		sample->send32 |= sio->p->memory.io[GBA_REG(SIODATA32_HI)] << 16;
		break;
	default:
		return false;
	}
	return true;
}

static bool _sendTransferSample(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, uint32_t sequence, enum GBASIOMode mode, const struct NetPlayTransferSample* sample, int32_t startCycle, bool hasStartCycle) {
	uint8_t payload[20];
	size_t payloadSize = 16;
	int playerId = driver->playerId >= 0 ? driver->playerId : 0;

	memset(payload, 0, sizeof(payload));
	_write32BE(&payload[0], sequence);
	payload[4] = _modeToWire(mode);
	payload[5] = playerId;
	_write16BE(&payload[6], sample->siocnt);
	_write16BE(&payload[8], sample->send16);
	_write32BE(&payload[12], sample->send32);
	if (hasStartCycle) {
		_write32BE(&payload[16], (uint32_t) startCycle);
		payloadSize = 20;
	}
	_logTransferControlSnapshot(driver, type == MSG_TRANSFER_START ? "START_TX" : "DATA_TX", sequence, mode, sample->siocnt);
	if (hasStartCycle) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: sending packet type=%u seq=%u mode=%u player=%d siocnt=%04X send16=%04X send32=%08X startCycle=%08X",
			type, (unsigned) sequence, _modeToWire(mode), playerId, sample->siocnt, sample->send16, sample->send32, (unsigned) startCycle);
	} else {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: sending packet type=%u seq=%u mode=%u player=%d siocnt=%04X send16=%04X send32=%08X",
			type, (unsigned) sequence, _modeToWire(mode), playerId, sample->siocnt, sample->send16, sample->send32);
	}
	return _sendPacket(driver, type, payload, payloadSize);
}

static bool _waitForTransferResult(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct GBASIONetPlayLockstepTransferResult* out, int32_t timeoutMs) {
#ifdef DISABLE_THREADING
	UNUSED(driver);
	UNUSED(mode);
	UNUSED(out);
	UNUSED(timeoutMs);
	return false;
#else
	uint32_t expectedSequence;
	int32_t remainingMs = timeoutMs;
	bool timedOut = false;
	MutexLock(&driver->mutex);
	expectedSequence = driver->transferSequence;
	while (driver->connected && driver->waitingForTransfer) {
		if (_pendingResultQueuePopSequence(driver, expectedSequence, out)) {
			if (out->mode != mode) {
				mLOG(GBA_SIO, WARN, "Transfer mode mismatch: expected %u got %u",
				     _modeToWire(mode), _modeToWire(out->mode));
				driver->waitingForTransfer = false;
				driver->transferActive = false;
				ConditionWake(&driver->cond);
				MutexUnlock(&driver->mutex);
				return false;
			}
			driver->waitingForTransfer = false;
			driver->transferActive = false;
			ConditionWake(&driver->cond);
			MutexUnlock(&driver->mutex);
			return true;
		}
		if (remainingMs >= 0) {
			int32_t waitMs;
			if (remainingMs <= 0) {
				timedOut = true;
				break;
			}
			waitMs = remainingMs > NETPLAY_HOST_MULTI_FINISH_WAIT_STEP_MS
				? NETPLAY_HOST_MULTI_FINISH_WAIT_STEP_MS
				: remainingMs;
			ConditionWaitTimed(&driver->cond, &driver->mutex, waitMs);
			remainingMs -= waitMs;
		} else {
			ConditionWait(&driver->cond, &driver->mutex);
		}
	}
	if (timedOut && driver->connected && driver->waitingForTransfer) {
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: timed out waiting for transfer result (seq=%u, mode=%u, timeout=%dms)",
		     (unsigned) expectedSequence, _modeToWire(mode), timeoutMs);
	}
	driver->waitingForTransfer = false;
	driver->transferActive = false;
	ConditionWake(&driver->cond);
	MutexUnlock(&driver->mutex);
	return false;
#endif
}

static bool _pollMultiplayerSecondaryFinish(struct GBASIONetPlayLockstepDriver* driver, uint16_t data[4]) {
#ifdef DISABLE_THREADING
	UNUSED(driver);
	memset(data, 0xFF, sizeof(uint16_t) * 4);
	return true;
#else
	struct GBASIONetPlayLockstepTransferResult result;
	uint32_t sequence;

	memset(data, 0xFF, sizeof(uint16_t) * 4);
	memset(&result, 0, sizeof(result));

	MutexLock(&driver->mutex);
	sequence = driver->transferSequence;

	if (!driver->connected) {
		driver->waitingForTransfer = false;
		driver->transferActive = false;
		driver->deferredMultiplayerResultValid = false;
		ConditionWake(&driver->cond);
		MutexUnlock(&driver->mutex);
		return true;
	}

	if (driver->waitingForTransfer) {
		if (!_pendingResultQueuePopSequence(driver, sequence, &result)) {
			MutexUnlock(&driver->mutex);
			return false;
		}
		if (result.mode != GBA_SIO_MULTI) {
			driver->waitingForTransfer = false;
			driver->transferActive = false;
			driver->deferredMultiplayerResultValid = false;
			ConditionWake(&driver->cond);
			MutexUnlock(&driver->mutex);
			_setDisconnected(driver, true);
			return true;
		}
		driver->waitingForTransfer = false;
		driver->transferActive = false;
		driver->deferredMultiplayerResultValid = false;
		ConditionWake(&driver->cond);
		MutexUnlock(&driver->mutex);
		memcpy(data, result.multiData, sizeof(uint16_t) * 4);
		return true;
	}

	MutexUnlock(&driver->mutex);
	return false;
#endif
}

static void GBASIONetPlayLockstepDriverFinishMultiplayer(struct GBASIODriver* driver, uint16_t data[4]) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	struct GBASIONetPlayLockstepTransferResult result;
	int32_t waitTimeoutMs = NETPLAY_HOST_MULTI_FINISH_TIMEOUT_MS;
	memset(data, 0xFF, sizeof(uint16_t) * 4);
	if (_waitForTransferResult(net, GBA_SIO_MULTI, &result, waitTimeoutMs)) {
		memcpy(data, result.multiData, sizeof(uint16_t) * 4);
	} else if (GBASIONetPlayLockstepDriverIsConnected(net)) {
		_setDisconnected(net, true);
	}
}

static bool GBASIONetPlayLockstepDriverFinishMultiplayerPoll(struct GBASIODriver* driver, uint16_t data[4]) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	if (GBASIONetPlayLockstepDriverDeviceId(driver) == 0) {
		/*
		 * Keep primary/host semantics blocking so games that expect transfer
		 * completion before progressing link state do not time out internally.
		 */
		GBASIONetPlayLockstepDriverFinishMultiplayer(driver, data);
		return true;
	}
	/*
	 * Keep secondary/client semantics non-blocking so input/render can progress
	 * while waiting for RESULT.
	 */
	return _pollMultiplayerSecondaryFinish(net, data);
}

static uint32_t GBASIONetPlayLockstepDriverFinishMultiplayerPollInterval(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	uint32_t interval = NETPLAY_MULTI_FINISH_POLL_WAIT_CYCLES;
#ifndef DISABLE_THREADING
	MutexLock(&net->mutex);
#endif
	if (!net->connected) {
		interval = NETPLAY_MULTI_FINISH_POLL_READY_CYCLES;
	} else if (_hasPendingResultForTransfer(net, net->transferSequence)) {
		interval = NETPLAY_MULTI_FINISH_POLL_READY_CYCLES;
	} else if (!net->waitingForTransfer && !net->transferActive) {
		interval = EVENT_ACTIVE_INTERVAL;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&net->mutex);
#endif
	return interval;
}

static uint8_t GBASIONetPlayLockstepDriverFinishNormal8(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	struct GBASIONetPlayLockstepTransferResult result;
	int playerId = GBASIONetPlayLockstepDriverDeviceId(driver);
	if (!_waitForTransferResult(net, GBA_SIO_NORMAL_8, &result, -1)) {
		if (GBASIONetPlayLockstepDriverIsConnected(net)) {
			_setDisconnected(net, true);
		}
		return 0xFF;
	}
	if (playerId > 0 && playerId < MAX_GBAS) {
		return result.normalData[playerId - 1] & 0xFF;
	}
	return 0xFF;
}

static uint32_t GBASIONetPlayLockstepDriverFinishNormal32(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	struct GBASIONetPlayLockstepTransferResult result;
	int playerId = GBASIONetPlayLockstepDriverDeviceId(driver);
	if (!_waitForTransferResult(net, GBA_SIO_NORMAL_32, &result, -1)) {
		if (GBASIONetPlayLockstepDriverIsConnected(net)) {
			_setDisconnected(net, true);
		}
		return 0xFFFFFFFF;
	}
	if (playerId > 0 && playerId < MAX_GBAS) {
		return result.normalData[playerId - 1];
	}
	return 0xFFFFFFFF;
}

static void _updateReadyState(struct GBASIONetPlayLockstepDriver* driver) {
	struct GBASIO* sio = driver->d.p;
	bool ready = true;
	bool present[MAX_GBAS] = { false, false, false, false };
	enum GBASIOMode modes[MAX_GBAS] = { -1, -1, -1, -1 };
	enum GBASIOMode mode;
	int playerId;
	int attached;
	int i;
	if (!sio) {
		return;
	}
#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	mode = driver->mode;
	playerId = driver->playerId;
	attached = driver->attached;
	for (i = 0; i < MAX_GBAS; ++i) {
		present[i] = driver->present[i];
		modes[i] = driver->otherModes[i];
	}
	if (playerId >= 0 && playerId < MAX_GBAS) {
		present[playerId] = true;
		modes[playerId] = mode;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif

	for (i = 0; i < MAX_GBAS; ++i) {
		if (!present[i]) {
			continue;
		}
		if (modes[i] != mode) {
			ready = false;
			break;
		}
	}
	if (mode == GBA_SIO_MULTI) {
		if (playerId < 0 || playerId >= MAX_GBAS) {
			playerId = 0;
		}
		if (attached < 1) {
			attached = 1;
		}
		sio->siocnt = GBASIOMultiplayerSetId(sio->siocnt, playerId);
		sio->siocnt = GBASIOMultiplayerSetSlave(sio->siocnt, playerId || attached < 2);
		sio->siocnt = GBASIOMultiplayerSetReady(sio->siocnt, ready);
		sio->rcnt = GBASIORegisterRCNTSetSd(sio->rcnt, ready);
	}
}

static void _syncSIOCNTFromBegin(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, uint16_t beginSIOCNT) {
	struct GBASIO* sio = driver->d.p;
	uint16_t copyMask = 0;

	if (!sio || !beginSIOCNT) {
		return;
	}

	switch (mode) {
	case GBA_SIO_MULTI:
		/* Sync transfer-timing bits only; keep local identity/ready bits. */
		copyMask = 0x4003; /* IRQ + baud */
		sio->siocnt = (sio->siocnt & ~copyMask) | (beginSIOCNT & copyMask);
		_updateReadyState(driver);
		return;
	case GBA_SIO_NORMAL_8:
	case GBA_SIO_NORMAL_32:
		copyMask = 0x5003; /* IRQ + length + clock select */
		sio->siocnt = (sio->siocnt & ~copyMask) | (beginSIOCNT & copyMask);
		return;
	default:
		return;
	}
}

static void _logTransferControlSnapshot(struct GBASIONetPlayLockstepDriver* driver, const char* phase, uint32_t sequence, enum GBASIOMode mode, uint16_t packetSIOCNT) {
#if !NETPLAY_VERBOSE_TRANSFER_TRACE
	UNUSED(driver);
	UNUSED(phase);
	UNUSED(sequence);
	UNUSED(mode);
	UNUSED(packetSIOCNT);
	return;
#else
	struct GBASIO* sio = driver->d.p;
	uint16_t localSIOCNT = sio ? sio->siocnt : 0;
	uint16_t localRCNT = sio ? sio->rcnt : 0;
	int playerId = driver->playerId;

	if (mode == GBA_SIO_MULTI) {
		unsigned pktBaud = packetSIOCNT & 0x3;
		unsigned pktBusy = (packetSIOCNT >> 7) & 1;
		unsigned pktReady = (packetSIOCNT >> 3) & 1;
		unsigned pktId = (packetSIOCNT >> 4) & 0x3;
		unsigned pktErr = (packetSIOCNT >> 6) & 1;
		unsigned pktIrq = (packetSIOCNT >> 14) & 1;
		unsigned localBaud = localSIOCNT & 0x3;
		unsigned localBusy = (localSIOCNT >> 7) & 1;
		unsigned localReady = (localSIOCNT >> 3) & 1;
		unsigned localId = (localSIOCNT >> 4) & 0x3;
		unsigned localErr = (localSIOCNT >> 6) & 1;
		unsigned localIrq = (localSIOCNT >> 14) & 1;
		unsigned rcntHi = (localRCNT >> 14) & 0x3;
		unsigned rcntPins = localRCNT & 0x000F;
		unsigned rcntDirs = (localRCNT >> 4) & 0x000F;
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: xfer %u %s p=%d MULTI pktSIOCNT=%04X(baud=%u busy=%u ready=%u id=%u err=%u irq=%u) localSIOCNT=%04X(baud=%u busy=%u ready=%u id=%u err=%u irq=%u) RCNT=%04X(hi=%u pins=%X dir=%X)",
		     (unsigned) sequence, phase, playerId, packetSIOCNT,
		     pktBaud, pktBusy, pktReady, pktId, pktErr, pktIrq,
		     localSIOCNT, localBaud, localBusy, localReady, localId, localErr, localIrq,
		     localRCNT, rcntHi, rcntPins, rcntDirs);
		return;
	}

	if (mode == GBA_SIO_NORMAL_8 || mode == GBA_SIO_NORMAL_32) {
		unsigned pktStart = (packetSIOCNT >> 7) & 1;
		unsigned pktLen = (packetSIOCNT >> 12) & 1;
		unsigned pktScInt = (packetSIOCNT >> 1) & 1;
		unsigned pktIrq = (packetSIOCNT >> 14) & 1;
		unsigned localStart = (localSIOCNT >> 7) & 1;
		unsigned localLen = (localSIOCNT >> 12) & 1;
		unsigned localScInt = (localSIOCNT >> 1) & 1;
		unsigned localIrq = (localSIOCNT >> 14) & 1;
		unsigned rcntHi = (localRCNT >> 14) & 0x3;
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: xfer %u %s p=%d NORMAL pktSIOCNT=%04X(start=%u len=%u intSc=%u irq=%u) localSIOCNT=%04X(start=%u len=%u intSc=%u irq=%u) RCNT=%04X(hi=%u)",
		     (unsigned) sequence, phase, playerId, packetSIOCNT,
		     pktStart, pktLen, pktScInt, pktIrq,
		     localSIOCNT, localStart, localLen, localScInt, localIrq,
		     localRCNT, rcntHi);
	}
#endif
}

static void _netPlayEvent(struct mTiming* timing, void* context, uint32_t cyclesLate) {
	struct GBASIONetPlayLockstepDriver* driver = context;
	bool pendingDisconnect;
	bool hasPendingBegin;
	bool transferActive;
	bool waitingForTransfer;
	bool hasPendingResult;
	bool clearPendingBegin = false;
	bool stateDirty;
	bool idChanged;
	bool connected;
	int playerId;
	struct GBASIONetPlayLockstepPendingBegin queuedBegin;
	uint32_t beginSequence = 0;
	enum GBASIOMode beginMode = (enum GBASIOMode) -1;
	uint8_t beginAttached = 0;
	uint16_t beginSIOCNT = 0;
	uint32_t sampleWriteGeneration = 0;
	UNUSED(cyclesLate);

#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	pendingDisconnect = driver->pendingDisconnect;
	hasPendingBegin = _pendingBeginQueuePeek(driver, &queuedBegin);
	transferActive = driver->transferActive;
	waitingForTransfer = driver->waitingForTransfer;
	hasPendingResult = _hasPendingResultForTransfer(driver, driver->transferSequence);
	stateDirty = driver->stateDirty;
	idChanged = driver->playerIdChanged;
	connected = driver->connected;
	playerId = driver->playerId;
	if (pendingDisconnect) {
		driver->pendingDisconnect = false;
	}
	if (hasPendingBegin) {
		beginSequence = queuedBegin.sequence;
		beginMode = queuedBegin.mode;
		beginAttached = queuedBegin.attached;
		beginSIOCNT = queuedBegin.siocnt;
	}
	if (stateDirty) {
		driver->stateDirty = false;
	}
	if (idChanged) {
		driver->playerIdChanged = false;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif

	if (pendingDisconnect) {
		mLOG(GBA_SIO, ERROR, "Relay connection closed");
	}
	if (idChanged && driver->user && driver->user->playerIdChanged) {
		int playerId = GBASIONetPlayLockstepDriverDeviceId(&driver->d);
		driver->user->playerIdChanged(driver->user, playerId);
	}
	if (stateDirty || idChanged) {
		_updateReadyState(driver);
	}

	if (hasPendingBegin && beginMode != (enum GBASIOMode) -1) {
		if (playerId == 0 || !_supportsTransferMode(beginMode)) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin dropped (playerId=%d, mode=%u)",
			     (unsigned) beginSequence, playerId, _modeToWire(beginMode));
			clearPendingBegin = true;
		} else {
			/* Don't start a new transfer while the current one hasn't fully completed locally. */
			if (transferActive || waitingForTransfer || hasPendingResult) {
				_rescheduleDriverEvent(timing, driver, connected ? EVENT_WAIT_INTERVAL : EVENT_IDLE_INTERVAL);
				return;
			}
			NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin pending (mode=%u, playerId=%d, attached=%u)",
			     (unsigned) beginSequence, _modeToWire(beginMode), playerId, beginAttached);
			struct NetPlayTransferSample sample;
			struct GBASIO* sio = driver->d.p;
			int32_t localCycle = mTimingCurrentTime(timing);
			int32_t freshnessElapsed = 0;
			bool waitingForFreshSample = false;
			uint32_t writeGeneration = 0;
			uint32_t lastSentGeneration = 0;
			if (sio && sio->mode != beginMode) {
				NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin mode mismatch (local=%u begin=%u), forcing local mode",
				     (unsigned) beginSequence, _modeToWire(sio->mode), _modeToWire(beginMode));
				sio->mode = beginMode;
#ifndef DISABLE_THREADING
				MutexLock(&driver->mutex);
#endif
				driver->mode = beginMode;
				if (driver->playerId >= 0 && driver->playerId < MAX_GBAS) {
					driver->otherModes[driver->playerId] = beginMode;
				}
#ifndef DISABLE_THREADING
				MutexUnlock(&driver->mutex);
#endif
			}
			_syncSIOCNTFromBegin(driver, beginMode, beginSIOCNT);
	#ifndef DISABLE_THREADING
			MutexLock(&driver->mutex);
	#endif
			writeGeneration = _sampleWriteGenerationForMode(driver, beginMode);
			lastSentGeneration = _sampleLastSentGenerationForMode(driver, beginMode);
			if (!driver->freshnessWaitActive
					|| driver->freshnessWaitSequence != beginSequence
					|| driver->freshnessWaitMode != beginMode) {
				driver->freshnessWaitActive = true;
				driver->freshnessWaitLogged = false;
				driver->freshnessWaitSequence = beginSequence;
				driver->freshnessWaitMode = beginMode;
				driver->freshnessWaitBaselineGeneration = lastSentGeneration;
				driver->freshnessWaitStartCycle = localCycle;
			}
			freshnessElapsed = localCycle - driver->freshnessWaitStartCycle;
			waitingForFreshSample = _requiresStrictFreshSample(beginMode)
				&& !_isSampleGenerationNewer(writeGeneration, lastSentGeneration);
			if (waitingForFreshSample && !driver->freshnessWaitLogged) {
				driver->freshnessWaitLogged = true;
				NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u waiting for fresh sample write (mode=%u lastSentGen=%u currentGen=%u)",
				     (unsigned) beginSequence, _modeToWire(beginMode),
				     (unsigned) lastSentGeneration, (unsigned) writeGeneration);
			}
	#ifndef DISABLE_THREADING
			MutexUnlock(&driver->mutex);
	#endif
			if (waitingForFreshSample) {
				int32_t remainingCycles = NETPLAY_MULTI_SAME_GENERATION_FALLBACK_WAIT_CYCLES - freshnessElapsed;
				if (remainingCycles > 0) {
					uint32_t waitCycles = (uint32_t) remainingCycles;
					if (!waitCycles || waitCycles > EVENT_WAIT_INTERVAL) {
						waitCycles = EVENT_WAIT_INTERVAL;
					}
					_rescheduleDriverEvent(timing, driver, connected ? waitCycles : EVENT_IDLE_INTERVAL);
					return;
				}
				NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u no fresh sample after %u cycles; reusing current value",
				     (unsigned) beginSequence,
				     (unsigned) NETPLAY_MULTI_SAME_GENERATION_FALLBACK_WAIT_CYCLES);
			}
			sampleWriteGeneration = writeGeneration;
			if (_captureTransferSample(driver, beginMode, &sample)) {
				int transferCycles = 1;
				int connectedDevices = beginAttached > 0 ? beginAttached - 1 : GBASIONetPlayLockstepDriverConnectedDevices(&driver->d);
				if (connectedDevices < 0) {
					connectedDevices = 0;
				}
				transferCycles = GBASIOTransferCycles(beginMode, beginSIOCNT ? beginSIOCNT : (sio ? sio->siocnt : 0), connectedDevices);
				if (transferCycles <= 0) {
					transferCycles = 1;
				}
#ifndef DISABLE_THREADING
				MutexLock(&driver->mutex);
#endif
				driver->waitingForTransfer = true;
				driver->transferActive = true;
				driver->transferSequence = beginSequence;
				driver->deferredMultiplayerResultValid = false;
#ifndef DISABLE_THREADING
				MutexUnlock(&driver->mutex);
#endif
				{
					bool sendOk = _sendTransferSample(driver, MSG_TRANSFER_DATA, beginSequence, beginMode, &sample, 0, false);
					if (sendOk) {
						if (sio) {
							sio->siocnt |= 0x80;
							mTimingDeschedule(&sio->p->timing, &sio->completeEvent);
							mTimingSchedule(&sio->p->timing, &sio->completeEvent, transferCycles);
						}
						NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin handled by player %d",
						     (unsigned) beginSequence, playerId);
	#ifndef DISABLE_THREADING
						MutexLock(&driver->mutex);
	#endif
						_setSampleLastSentGenerationForMode(driver, beginMode, sampleWriteGeneration);
						driver->multiSameGenerationFastPathActive = false;
						driver->multiSameGenerationFastPathGeneration = sampleWriteGeneration;
						_clearFreshnessWait(driver);
	#ifndef DISABLE_THREADING
						MutexUnlock(&driver->mutex);
	#endif
						clearPendingBegin = true;
					} else {
	#ifndef DISABLE_THREADING
						MutexLock(&driver->mutex);
	#endif
						if (driver->transferSequence == beginSequence) {
							driver->waitingForTransfer = false;
							driver->transferActive = false;
						}
						_clearFreshnessWait(driver);
	#ifndef DISABLE_THREADING
						ConditionWake(&driver->cond);
						MutexUnlock(&driver->mutex);
	#endif
						mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin send failed (playerId=%d, mode=%u)",
						     (unsigned) beginSequence, playerId, _modeToWire(beginMode));
						clearPendingBegin = true;
						_setDisconnected(driver, true);
					}
				}
			} else {
				mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin deferred (sample unavailable, playerId=%d, sio=%p)",
				     (unsigned) beginSequence, playerId, (void*) driver->d.p);
			}
		}
	}

	if (clearPendingBegin) {
#ifndef DISABLE_THREADING
		MutexLock(&driver->mutex);
#endif
			if (driver->pendingBeginCount && driver->pendingBegins[driver->pendingBeginRead].sequence == beginSequence) {
				_pendingBeginQueuePop(driver);
#ifndef DISABLE_THREADING
				ConditionWake(&driver->cond);
#endif
			}
#ifndef DISABLE_THREADING
		MutexUnlock(&driver->mutex);
#endif
	}

#ifndef DISABLE_THREADING
	{
		uint32_t nextInterval = EVENT_ACTIVE_INTERVAL;
		MutexLock(&driver->mutex);
		connected = driver->connected;
		if (!connected) {
			nextInterval = EVENT_IDLE_INTERVAL;
		} else if (driver->playerId > 0 && driver->pendingBeginCount) {
			nextInterval = EVENT_BUSY_INTERVAL;
		}
		driver->asleep = false;
		MutexUnlock(&driver->mutex);
		_rescheduleDriverEvent(timing, driver, nextInterval);
		return;
	}
#else
	_rescheduleDriverEvent(timing, driver, connected ? EVENT_ACTIVE_INTERVAL : EVENT_IDLE_INTERVAL);
#endif
}

#ifndef DISABLE_THREADING
static bool _recvAll(Socket socket, void* out, size_t size) {
	size_t got = 0;
	ssize_t ret;
	uint8_t* buffer = out;
	if (SOCKET_FAILED(socket)) {
		return false;
	}
	while (got < size) {
		ret = SocketRecv(socket, &buffer[got], size - got);
		if (ret <= 0) {
			return false;
		}
		got += ret;
	}
	return true;
}

static bool _handleStatePacket(struct GBASIONetPlayLockstepDriver* driver, const uint8_t* payload, size_t size) {
	uint8_t presentMask;
	int attached = 0;
	int oldPlayerId;
	int i;
	struct mLockstepUser* user = NULL;
	if (size < 8) {
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: STATE packet too small (%u)", (unsigned) size);
		return false;
	}
	presentMask = payload[2];
	for (i = 0; i < MAX_GBAS; ++i) {
		attached += !!(presentMask & (1 << i));
	}
	MutexLock(&driver->mutex);
	oldPlayerId = driver->playerId;
	driver->playerId = (payload[0] < MAX_GBAS) ? payload[0] : -1;
	driver->playerIdChanged = oldPlayerId != driver->playerId;
	driver->attached = attached > 0 ? attached : 1;
	for (i = 0; i < MAX_GBAS; ++i) {
		driver->present[i] = !!(presentMask & (1 << i));
		driver->otherModes[i] = _modeFromWire(payload[4 + i]);
	}
	if (driver->playerId >= 0 && driver->playerId < MAX_GBAS) {
		driver->otherModes[driver->playerId] = driver->mode;
	}
	driver->stateDirty = true;
	user = _wakeDriverLocked(driver);
	ConditionWake(&driver->cond);
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: state update playerId=%d attached=%d presentMask=%02X",
	     driver->playerId, driver->attached, presentMask);
	MutexUnlock(&driver->mutex);
	if (user) {
		user->wake(user);
	}
	return true;
}

static bool _handleTransferBeginPacket(struct GBASIONetPlayLockstepDriver* driver, const uint8_t* payload, size_t size) {
	bool transferInFlight;
	bool validMode;
	uint32_t sequence;
	int32_t startCycle = 0;
	bool hasStartCycle = false;
	enum GBASIOMode mode;
	struct mLockstepUser* user = NULL;
	if (size < 10) {
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: TRANSFER_BEGIN packet too small (%u)", (unsigned) size);
		return false;
	}
	sequence = _read32BE(&payload[0]);
	mode = _modeFromWire(payload[4]);
	hasStartCycle = size >= 16;
	if (hasStartCycle) {
		startCycle = (int32_t) _read32BE(&payload[12]);
	}
	MutexLock(&driver->mutex);
	transferInFlight = driver->transferActive
		|| driver->waitingForTransfer
		|| _hasPendingResultForTransfer(driver, driver->transferSequence);
	if (transferInFlight && driver->transferSequence == sequence) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: ignoring duplicate transfer %u begin while active",
		     (unsigned) sequence);
		MutexUnlock(&driver->mutex);
		return true;
	}
	if (_pendingBeginQueueContainsSequence(driver, sequence)) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: ignoring duplicate transfer %u begin while queued",
		     (unsigned) sequence);
		MutexUnlock(&driver->mutex);
		return true;
	}

	validMode = mode != (enum GBASIOMode) -1;
	if (!validMode) {
		MutexUnlock(&driver->mutex);
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: TRANSFER_BEGIN packet has invalid mode (%u)", payload[4]);
		return false;
	}

	if (!_pendingBeginQueuePush(driver, sequence, mode, payload[6], _read16BE(&payload[8]), startCycle, hasStartCycle)) {
		mLOG(GBA_SIO, ERROR, "NetPlay lockstep: TRANSFER_BEGIN queue overflow at transfer %u (depth=%u)",
		     (unsigned) sequence, (unsigned) driver->pendingBeginCount);
		MutexUnlock(&driver->mutex);
		_setDisconnected(driver, true);
		return false;
	}

	user = _wakeDriverLocked(driver);
	ConditionWake(&driver->cond);
	if (transferInFlight) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin queued while transfer %u is active",
		     (unsigned) sequence, (unsigned) driver->transferSequence);
	} else {
		if (hasStartCycle) {
			NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin received (mode=%u, attached=%u, startCycle=%08X)",
			     (unsigned) sequence, payload[4], payload[6], (unsigned) (uint32_t) startCycle);
		} else {
			NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin received (mode=%u, attached=%u)",
			     (unsigned) sequence, payload[4], payload[6]);
		}
	}
	NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer %u begin queued (playerId=%d, depth=%u, sio=%p)",
	     (unsigned) sequence, driver->playerId, (unsigned) driver->pendingBeginCount, (void*) driver->d.p);
	MutexUnlock(&driver->mutex);
	if (user) {
		user->wake(user);
	}
	return true;
}

static bool _handleTransferResultPacket(struct GBASIONetPlayLockstepDriver* driver, const uint8_t* payload, size_t size) {
	struct GBASIONetPlayLockstepTransferResult result;
	int i;
	uint32_t sequence;
	struct mLockstepUser* user = NULL;
	if (size < 32) {
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: TRANSFER_RESULT packet too small (%u)", (unsigned) size);
		return false;
	}
	sequence = _read32BE(&payload[0]);
	result.sequence = sequence;
	result.mode = _modeFromWire(payload[4]);
	result.attached = payload[5];
	for (i = 0; i < MAX_GBAS; ++i) {
		result.multiData[i] = _read16BE(&payload[8 + i * 2]);
		result.normalData[i] = _read32BE(&payload[16 + i * 4]);
	}

	MutexLock(&driver->mutex);
	if (_pendingResultQueueContainsSequence(driver, sequence)) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: ignoring duplicate transfer result for sequence %u",
		     (unsigned) sequence);
		MutexUnlock(&driver->mutex);
		return true;
	}
	if (!_pendingResultQueuePush(driver, &result)) {
		mLOG(GBA_SIO, ERROR, "NetPlay lockstep: TRANSFER_RESULT queue overflow at transfer %u (depth=%u)",
		     (unsigned) sequence, (unsigned) driver->pendingResultCount);
		MutexUnlock(&driver->mutex);
		_setDisconnected(driver, true);
		return false;
	}
	if (!driver->transferActive || !driver->waitingForTransfer) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: transfer result %u queued while no transfer is active",
		     (unsigned) sequence);
	}
	user = _wakeDriverLocked(driver);
	ConditionWake(&driver->cond);
	MutexUnlock(&driver->mutex);
	if (user) {
		user->wake(user);
	}
	return true;
}

static bool _handleIncomingPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size) {
	bool handled = false;
	NETPLAY_TRANSFER_TRACE("NetPlay lockstep: received packet type=%u size=%u", type, (unsigned) size);
	switch (type) {
	case MSG_STATE:
		handled = _handleStatePacket(driver, payload, size);
		break;
	case MSG_TRANSFER_BEGIN:
		handled = _handleTransferBeginPacket(driver, payload, size);
		break;
	case MSG_TRANSFER_RESULT:
		handled = _handleTransferResultPacket(driver, payload, size);
		break;
	default:
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: unknown packet type=%u size=%u", type, (unsigned) size);
		handled = false;
		break;
	}
	if (handled) {
		NETPLAY_TRANSFER_TRACE("NetPlay lockstep: processed packet type=%u successfully", type);
	}
	return handled;
}

static THREAD_ENTRY _readerThread(void* context) {
	struct GBASIONetPlayLockstepDriver* driver = context;
	uint8_t header[8];
	uint8_t payload[MAX_PACKET_SIZE];
	uint32_t size;
	Socket reads[1];
	int pollResult = 0;
	bool stopping = false;
	struct mLogger* logger = NULL;
	Socket socket = INVALID_SOCKET;

	MutexLock(&driver->mutex);
	logger = driver->readerLogger;
	socket = driver->socket;
	MutexUnlock(&driver->mutex);
	if (logger) {
		mLogSetThreadLogger(logger);
	}

	ThreadSetName("NetPlay Relay");
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: relay reader thread running");
	while (true) {
		reads[0] = socket;
		/*
		 * Keep RX poll non-blocking so freshly queued outbound packets can be
		 * flushed with minimal added latency. Idle backoff is handled via cond.
		 */
		pollResult = SocketPoll(1, reads, NULL, NULL, 0);
		if (pollResult < 0) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: reader poll failed");
			break;
		}
		if (!pollResult || SOCKET_FAILED(reads[0])) {
			MutexLock(&driver->mutex);
			if (driver->connected && !driver->stopping) {
				ConditionWaitTimed(&driver->cond, &driver->mutex, READER_IDLE_WAIT_MS);
			}
			MutexUnlock(&driver->mutex);
			continue;
		}
		if (!_recvAll(socket, header, sizeof(header))) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: reader failed to read packet header");
			break;
		}
		size = _read32BE(&header[4]);
		if (size > sizeof(payload)) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: reader got oversized packet (%u)", (unsigned) size);
			break;
		}
		if (size && !_recvAll(socket, payload, size)) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: reader failed to read payload (%u)", (unsigned) size);
			break;
		}
		_handleIncomingPacket(driver, header[0], payload, size);
	}

	MutexLock(&driver->mutex);
	stopping = driver->stopping;
	driver->connected = false;
	driver->waitingForTransfer = false;
	driver->transferActive = false;
	driver->deferredMultiplayerResultValid = false;
	driver->cycleSyncValid = false;
	driver->cycleSyncOffset = 0;
	driver->cycleSyncSequence = 0;
	driver->multiSendWriteGeneration = 0;
	driver->normal8WriteGeneration = 0;
	driver->normal32WriteGeneration = 0;
	driver->multiSendWriteHistoryWrite = 0;
	driver->multiSendWriteHistoryCount = 0;
	driver->multiSendLastSentGeneration = UINT32_MAX;
	driver->normal8LastSentGeneration = UINT32_MAX;
	driver->normal32LastSentGeneration = UINT32_MAX;
	_clearFreshnessWait(driver);
	driver->multiSameGenerationFastPathActive = false;
	driver->multiSameGenerationFastPathGeneration = 0;
	driver->asleep = false;
	driver->clientIdleEvents = 0;
	driver->pendingResultRead = 0;
	driver->pendingResultWrite = 0;
	driver->pendingResultCount = 0;
	if (!stopping) {
		driver->pendingDisconnect = true;
	}
	ConditionWake(&driver->cond);
	MutexUnlock(&driver->mutex);
	_wakeDriver(driver);
	THREAD_EXIT(0);
}
#endif
