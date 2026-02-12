/* Copyright (c) 2026-2026 Maik Mursall
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
#include <mgba/internal/gba/sio/netplay-lockstep.h>

#include <mgba/internal/gba/gba.h>
#include <mgba/internal/gba/io.h>

#define DRIVER_ID 0x4E506C73
#define EVENT_IDLE_INTERVAL 1024
#define EVENT_ACTIVE_INTERVAL 64
#define MAX_PACKET_SIZE 512
#define CONNECT_ID_WAIT_MS 5000

#define NETPLAY_CLIENT_PACING_NONE 0
#define NETPLAY_CLIENT_PACING_SOFT 1
#define NETPLAY_CLIENT_PACING_HARD 2

#ifndef NETPLAY_CLIENT_PACING_MODE
#define NETPLAY_CLIENT_PACING_MODE NETPLAY_CLIENT_PACING_NONE
#endif

#define NETPLAY_CLIENT_SOFT_IDLE_CYCLES 280896
#define NETPLAY_CLIENT_SOFT_IDLE_EVENTS ((NETPLAY_CLIENT_SOFT_IDLE_CYCLES + EVENT_ACTIVE_INTERVAL - 1) / EVENT_ACTIVE_INTERVAL)
#define NETPLAY_CLIENT_SOFT_WAIT_MS 5

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
static uint8_t GBASIONetPlayLockstepDriverFinishNormal8(struct GBASIODriver* driver);
static uint32_t GBASIONetPlayLockstepDriverFinishNormal32(struct GBASIODriver* driver);

static void _netPlayEvent(struct mTiming* timing, void* context, uint32_t cyclesLate);

static bool _sendPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size);
static void _setDisconnected(struct GBASIONetPlayLockstepDriver* driver, bool remoteClose);
static void _updateReadyState(struct GBASIONetPlayLockstepDriver* driver);
static void _syncSIOCNTFromBegin(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, uint16_t beginSIOCNT);
static void _logTransferControlSnapshot(struct GBASIONetPlayLockstepDriver* driver, const char* phase, uint32_t sequence, enum GBASIOMode mode, uint16_t packetSIOCNT);
static bool _captureTransferSample(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct NetPlayTransferSample* sample);
static bool _sendTransferSample(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, uint32_t sequence, enum GBASIOMode mode, const struct NetPlayTransferSample* sample, int32_t startCycle, bool hasStartCycle);
static bool _waitForTransferResult(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct GBASIONetPlayLockstepTransferResult* out);
static void _wakeDriver(struct GBASIONetPlayLockstepDriver* driver);
static void _paceClientSoft(struct GBASIONetPlayLockstepDriver* driver);
static void _sleepDriver(struct GBASIONetPlayLockstepDriver* driver);
static bool _tryGetLocalCycle(struct GBASIONetPlayLockstepDriver* driver, int32_t* outCycle);

#ifndef DISABLE_THREADING
static THREAD_ENTRY _readerThread(void* context);
static bool _recvAll(struct GBASIONetPlayLockstepDriver* driver, void* out, size_t size);
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
	uint8_t i;
	uint8_t idx;
	for (i = 0; i < driver->pendingBeginCount; ++i) {
		idx = (driver->pendingBeginRead + i) % NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE;
		if (driver->pendingBegins[idx].sequence == sequence) {
			return true;
		}
	}
	return false;
}

static bool _pendingBeginQueuePush(struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence, enum GBASIOMode mode, uint8_t attached, uint16_t siocnt, int32_t startCycle, bool hasStartCycle) {
	struct GBASIONetPlayLockstepPendingBegin* begin;
	if (driver->pendingBeginCount >= NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE) {
		return false;
	}
	begin = &driver->pendingBegins[driver->pendingBeginWrite];
	begin->sequence = sequence;
	begin->mode = mode;
	begin->attached = attached;
	begin->siocnt = siocnt;
	begin->startCycle = startCycle;
	begin->hasStartCycle = hasStartCycle;
	driver->pendingBeginWrite = (driver->pendingBeginWrite + 1) % NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE;
	++driver->pendingBeginCount;
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
	driver->pendingBeginRead = (driver->pendingBeginRead + 1) % NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE;
	--driver->pendingBeginCount;
}

static bool _pendingResultQueueContainsSequence(const struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence) {
	uint8_t i;
	uint8_t idx;
	for (i = 0; i < driver->pendingResultCount; ++i) {
		idx = (driver->pendingResultRead + i) % NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE;
		if (driver->pendingResults[idx].sequence == sequence) {
			return true;
		}
	}
	return false;
}

static bool _pendingResultQueuePush(struct GBASIONetPlayLockstepDriver* driver, const struct GBASIONetPlayLockstepTransferResult* result) {
	if (_pendingResultQueueContainsSequence(driver, result->sequence)) {
		return true;
	}
	if (driver->pendingResultCount >= NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE) {
		return false;
	}
	driver->pendingResults[driver->pendingResultWrite] = *result;
	driver->pendingResultWrite = (driver->pendingResultWrite + 1) % NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE;
	++driver->pendingResultCount;
	return true;
}

static bool _pendingResultQueuePopSequence(struct GBASIONetPlayLockstepDriver* driver, uint32_t sequence, struct GBASIONetPlayLockstepTransferResult* out) {
	struct GBASIONetPlayLockstepTransferResult reordered[NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE];
	uint8_t i;
	uint8_t idx;
	uint8_t kept = 0;
	bool found = false;

	if (!driver->pendingResultCount) {
		return false;
	}

	for (i = 0; i < driver->pendingResultCount; ++i) {
		idx = (driver->pendingResultRead + i) % NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE;
		if (!found && driver->pendingResults[idx].sequence == sequence) {
			if (out) {
				*out = driver->pendingResults[idx];
			}
			found = true;
			continue;
		}
		reordered[kept++] = driver->pendingResults[idx];
	}

	if (!found) {
		return false;
	}

	for (i = 0; i < kept; ++i) {
		driver->pendingResults[i] = reordered[i];
	}
	driver->pendingResultRead = 0;
	driver->pendingResultWrite = kept % NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE;
	driver->pendingResultCount = kept;
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
	ConditionInit(&driver->cond);
	driver->readerLogger = NULL;
#endif
}

void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver* driver) {
	_setDisconnected(driver, false);
#ifndef DISABLE_THREADING
	ConditionDeinit(&driver->cond);
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
	net->cycleSyncValid = false;
	net->cycleSyncOffset = 0;
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
	UNUSED(driver);
	UNUSED(address);
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
	driver->cycleSyncValid = false;
	driver->cycleSyncOffset = 0;
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
	driver->clientIdleEvents = 0;
	++driver->wakeGeneration;
	if (driver->user && driver->user->wake) {
		driver->asleep = false;
		user = driver->user;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif
	if (user) {
		user->wake(user);
	}
}

static void _paceClientSoft(struct GBASIONetPlayLockstepDriver* driver) {
#ifndef DISABLE_THREADING
	bool shouldWait = false;
	MutexLock(&driver->mutex);
	if (driver->connected
			&& driver->playerId > 0
			&& driver->attached > 1
			&& !driver->pendingBeginCount
			&& !driver->transferActive
			&& !driver->waitingForTransfer
			&& !_hasPendingResultForTransfer(driver, driver->transferSequence)) {
		/* Soft pacing should never hard-block: wait briefly, then resume. */
		driver->clientIdleEvents = 0;
		shouldWait = true;
	}
	if (shouldWait) {
		ConditionWaitTimed(&driver->cond, &driver->mutex, NETPLAY_CLIENT_SOFT_WAIT_MS);
	}
	MutexUnlock(&driver->mutex);
#else
	UNUSED(driver);
#endif
}

static void _sleepDriver(struct GBASIONetPlayLockstepDriver* driver) {
	struct mLockstepUser* user = NULL;
	uint32_t sleepGeneration = 0;
	bool cancelSleep = false;
#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	if (driver->user && driver->user->sleep && driver->user->wake
			&& !driver->asleep
			&& driver->connected
			&& driver->playerId > 0
			&& driver->attached > 1
			&& !driver->pendingBeginCount
			&& !driver->transferActive
			&& !driver->waitingForTransfer
			&& !_hasPendingResultForTransfer(driver, driver->transferSequence)) {
		driver->asleep = true;
		user = driver->user;
		sleepGeneration = driver->wakeGeneration;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif
	if (!user) {
		return;
	}

	user->sleep(user);
	if (driver->d.p && driver->d.p->p && driver->d.p->p->cpu) {
		driver->d.p->p->cpu->nextEvent = 0;
		GBAInterrupt(driver->d.p->p);
	}

#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	if (!driver->asleep
			|| driver->wakeGeneration != sleepGeneration
			|| !driver->connected
			|| driver->playerId <= 0
			|| driver->attached < 2
			|| driver->pendingBeginCount
			|| driver->transferActive
			|| driver->waitingForTransfer
			|| _hasPendingResultForTransfer(driver, driver->transferSequence)) {
		driver->asleep = false;
		cancelSleep = true;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif
	if (cancelSleep) {
		user->wake(user);
	}
}

static bool _sendPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size) {
	uint8_t header[8] = { 0 };
	size_t sent;
	ssize_t written;
	Socket socket;

	if (size > UINT32_MAX) {
		return false;
	}

#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	if (!driver->connected || SOCKET_FAILED(driver->socket)) {
#ifndef DISABLE_THREADING
		MutexUnlock(&driver->mutex);
#endif
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: send failed (disconnected) type=%u size=%u", type, (unsigned) size);
		return false;
	}
	socket = driver->socket;
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif

	header[0] = type;
	_write32BE(&header[4], (uint32_t) size);
	sent = 0;
	while (sent < sizeof(header)) {
		written = SocketSend(socket, &header[sent], sizeof(header) - sent);
		if (written <= 0) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: send header failed type=%u", type);
			return false;
		}
		sent += written;
	}

	sent = 0;
	while (sent < size) {
		written = SocketSend(socket, &payload[sent], size - sent);
		if (written <= 0) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: send payload failed type=%u", type);
			return false;
		}
		sent += written;
	}
	return true;
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
	driver->cycleSyncValid = false;
	driver->cycleSyncOffset = 0;
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
	int playerId = 0;
#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
#endif
	if (driver->playerId >= 0) {
		playerId = driver->playerId;
	}
#ifndef DISABLE_THREADING
	MutexUnlock(&driver->mutex);
#endif

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
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: sending packet type=%u seq=%u mode=%u player=%d siocnt=%04X send16=%04X send32=%08X startCycle=%08X",
		     type, (unsigned) sequence, _modeToWire(mode), playerId, sample->siocnt, sample->send16, sample->send32, (unsigned) startCycle);
	} else {
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: sending packet type=%u seq=%u mode=%u player=%d siocnt=%04X send16=%04X send32=%08X",
		     type, (unsigned) sequence, _modeToWire(mode), playerId, sample->siocnt, sample->send16, sample->send32);
	}
	return _sendPacket(driver, type, payload, payloadSize);
}

static bool _waitForTransferResult(struct GBASIONetPlayLockstepDriver* driver, enum GBASIOMode mode, struct GBASIONetPlayLockstepTransferResult* out) {
#ifdef DISABLE_THREADING
	UNUSED(driver);
	UNUSED(mode);
	UNUSED(out);
	return false;
#else
	uint32_t expectedSequence;
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
		ConditionWait(&driver->cond, &driver->mutex);
	}
	driver->waitingForTransfer = false;
	driver->transferActive = false;
	ConditionWake(&driver->cond);
	MutexUnlock(&driver->mutex);
	return false;
#endif
}

static void GBASIONetPlayLockstepDriverFinishMultiplayer(struct GBASIODriver* driver, uint16_t data[4]) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	struct GBASIONetPlayLockstepTransferResult result;
	memset(data, 0xFF, sizeof(uint16_t) * 4);
	if (_waitForTransferResult(net, GBA_SIO_MULTI, &result)) {
		memcpy(data, result.multiData, sizeof(uint16_t) * 4);
	}
}

static uint8_t GBASIONetPlayLockstepDriverFinishNormal8(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	struct GBASIONetPlayLockstepTransferResult result;
	int playerId = GBASIONetPlayLockstepDriverDeviceId(driver);
	if (!_waitForTransferResult(net, GBA_SIO_NORMAL_8, &result)) {
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
	if (!_waitForTransferResult(net, GBA_SIO_NORMAL_32, &result)) {
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
#if NETPLAY_CLIENT_PACING_MODE != NETPLAY_CLIENT_PACING_NONE && !defined(DISABLE_THREADING)
	bool shouldPaceClient = false;
#endif
	int playerId;
	struct GBASIONetPlayLockstepPendingBegin queuedBegin;
	uint32_t beginSequence = 0;
	enum GBASIOMode beginMode = (enum GBASIOMode) -1;
	uint8_t beginAttached = 0;
	uint16_t beginSIOCNT = 0;
	int32_t beginStartCycle = 0;
	int32_t beginTargetCycle = 0;
	int32_t beginCycleDelta = 0;
	bool beginCycleCompared = false;
	bool beginHasStartCycle = false;
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
		beginStartCycle = queuedBegin.startCycle;
		beginHasStartCycle = queuedBegin.hasStartCycle;
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
		/* Don't start a new transfer while the current one hasn't fully completed locally. */
		if (transferActive || waitingForTransfer || hasPendingResult) {
			mTimingSchedule(timing, &driver->event, connected ? EVENT_ACTIVE_INTERVAL : EVENT_IDLE_INTERVAL);
			return;
		}
		if (!beginHasStartCycle) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin dropped (missing start cycle metadata)",
			     (unsigned) beginSequence);
			clearPendingBegin = true;
			_setDisconnected(driver, true);
		} else {
			bool deferForCycle = false;
			bool calibratedCycleSync = false;
			int32_t localCycle = mTimingCurrentTime(timing);
			int32_t targetCycle = 0;
			int32_t untilStartCycle = 0;
#ifndef DISABLE_THREADING
			MutexLock(&driver->mutex);
#endif
			if (!driver->cycleSyncValid) {
				driver->cycleSyncOffset = localCycle - beginStartCycle;
				driver->cycleSyncValid = true;
				calibratedCycleSync = true;
			}
			targetCycle = beginStartCycle + driver->cycleSyncOffset;
			untilStartCycle = targetCycle - localCycle;
			deferForCycle = untilStartCycle > 0;
			beginTargetCycle = targetCycle;
			beginCycleDelta = untilStartCycle;
			beginCycleCompared = true;
#ifndef DISABLE_THREADING
			MutexUnlock(&driver->mutex);
#endif
			if (calibratedCycleSync) {
				mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: cycle sync calibrated offset=%08X (local=%08X start=%08X)",
				     (unsigned) (uint32_t) (localCycle - beginStartCycle),
				     (unsigned) (uint32_t) localCycle,
				     (unsigned) (uint32_t) beginStartCycle);
			}
			mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u cycle compare start=%08X local=%08X target=%08X delta=%d",
			     (unsigned) beginSequence,
			     (unsigned) (uint32_t) beginStartCycle,
			     (unsigned) (uint32_t) localCycle,
			     (unsigned) (uint32_t) targetCycle,
			     (int) beginCycleDelta);
			if (deferForCycle) {
				uint32_t waitCycles = (uint32_t) untilStartCycle;
				if (!waitCycles || waitCycles > EVENT_ACTIVE_INTERVAL) {
					waitCycles = EVENT_ACTIVE_INTERVAL;
				}
				mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin deferred until local cycle %08X (local=%08X, remaining=%u)",
				     (unsigned) beginSequence,
				     (unsigned) (uint32_t) targetCycle,
				     (unsigned) (uint32_t) localCycle,
				     (unsigned) waitCycles);
				mTimingSchedule(timing, &driver->event, connected ? waitCycles : EVENT_IDLE_INTERVAL);
				return;
			}
		}
		if (!clearPendingBegin) {
			mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin pending (mode=%u, playerId=%d, attached=%u, startCycle=%08X)",
			     (unsigned) beginSequence, _modeToWire(beginMode), playerId, beginAttached, (unsigned) (uint32_t) beginStartCycle);
			_logTransferControlSnapshot(driver, "BEGIN_RX", beginSequence, beginMode, beginSIOCNT);
		}
		if (!clearPendingBegin && playerId != 0 && _supportsTransferMode(beginMode)) {
			struct NetPlayTransferSample sample;
			struct GBASIO* sio = driver->d.p;
			if (sio && sio->mode != beginMode) {
				mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin mode mismatch (local=%u begin=%u), forcing local mode",
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
				/*
				 * Mark transfer active before sending DATA so an immediate RESULT
				 * is associated with this sequence while local finish is pending.
				 */
				driver->waitingForTransfer = true;
				driver->transferActive = true;
				driver->transferSequence = beginSequence;
#ifndef DISABLE_THREADING
				MutexUnlock(&driver->mutex);
#endif
				if (_sendTransferSample(driver, MSG_TRANSFER_DATA, beginSequence, beginMode, &sample, 0, false)) {
					if (sio) {
						if (beginCycleCompared) {
							int32_t sendLocalCycle = mTimingCurrentTime(&sio->p->timing);
							int32_t sendDelta = beginTargetCycle - sendLocalCycle;
							mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u DATA sample cycle local=%08X target=%08X delta=%d",
							     (unsigned) beginSequence,
							     (unsigned) (uint32_t) sendLocalCycle,
							     (unsigned) (uint32_t) beginTargetCycle,
							     (int) sendDelta);
						}
						sio->siocnt |= 0x80;
						mTimingDeschedule(&sio->p->timing, &sio->completeEvent);
						mTimingSchedule(&sio->p->timing, &sio->completeEvent, transferCycles);
					}
					mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin handled by player %d",
					     (unsigned) beginSequence, playerId);
					clearPendingBegin = true;
				} else {
#ifndef DISABLE_THREADING
					MutexLock(&driver->mutex);
#endif
					if (driver->transferSequence == beginSequence) {
						driver->waitingForTransfer = false;
						driver->transferActive = false;
					}
#ifndef DISABLE_THREADING
					ConditionWake(&driver->cond);
					MutexUnlock(&driver->mutex);
#endif
					mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin send failed (playerId=%d, mode=%u)",
					     (unsigned) beginSequence, playerId, _modeToWire(beginMode));
					clearPendingBegin = true;
					_setDisconnected(driver, true);
				}
			} else {
				mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin deferred (sample unavailable, playerId=%d, sio=%p)",
				     (unsigned) beginSequence, playerId, (void*) driver->d.p);
			}
		} else if (!clearPendingBegin && (playerId == 0 || !_supportsTransferMode(beginMode))) {
			// Primary should never receive begin packets; unsupported modes are ignored.
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: transfer %u begin dropped (playerId=%d, supported=%d, mode=%u)",
			     (unsigned) beginSequence, playerId, _supportsTransferMode(beginMode), _modeToWire(beginMode));
			clearPendingBegin = true;
		} else if (!clearPendingBegin) {
			mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin deferred (playerId=%d)",
			     (unsigned) beginSequence, playerId);
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

#if NETPLAY_CLIENT_PACING_MODE != NETPLAY_CLIENT_PACING_NONE
#ifndef DISABLE_THREADING
	MutexLock(&driver->mutex);
	if (driver->connected && driver->playerId > 0) {
		bool idle = !driver->pendingBeginCount
			&& !driver->transferActive
			&& !driver->waitingForTransfer
			&& !_hasPendingResultForTransfer(driver, driver->transferSequence)
			&& driver->attached > 1;
		if (idle) {
#if NETPLAY_CLIENT_PACING_MODE == NETPLAY_CLIENT_PACING_HARD
			shouldPaceClient = true;
#else
			if (driver->clientIdleEvents < NETPLAY_CLIENT_SOFT_IDLE_EVENTS) {
				++driver->clientIdleEvents;
			}
			shouldPaceClient = driver->clientIdleEvents >= NETPLAY_CLIENT_SOFT_IDLE_EVENTS;
#endif
		} else {
			driver->clientIdleEvents = 0;
		}
	} else {
		driver->clientIdleEvents = 0;
	}
	MutexUnlock(&driver->mutex);
#if NETPLAY_CLIENT_PACING_MODE == NETPLAY_CLIENT_PACING_HARD
	if (shouldPaceClient) {
		_sleepDriver(driver);
	}
#else
	if (shouldPaceClient) {
		_paceClientSoft(driver);
	}
#endif
#endif
#endif

	mTimingSchedule(timing, &driver->event, connected ? EVENT_ACTIVE_INTERVAL : EVENT_IDLE_INTERVAL);
}

#ifndef DISABLE_THREADING
static bool _recvAll(struct GBASIONetPlayLockstepDriver* driver, void* out, size_t size) {
	size_t got = 0;
	ssize_t ret;
	uint8_t* buffer = out;
	Socket socket;
	MutexLock(&driver->mutex);
	socket = driver->socket;
	MutexUnlock(&driver->mutex);
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
	int oldAttached;
	int i;
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
	oldAttached = driver->attached;
	driver->playerId = (payload[0] < MAX_GBAS) ? payload[0] : -1;
	driver->playerIdChanged = oldPlayerId != driver->playerId;
	driver->attached = attached > 0 ? attached : 1;
	if (driver->playerIdChanged || oldAttached != driver->attached) {
		driver->cycleSyncValid = false;
		driver->cycleSyncOffset = 0;
	}
	for (i = 0; i < MAX_GBAS; ++i) {
		driver->present[i] = !!(presentMask & (1 << i));
		driver->otherModes[i] = _modeFromWire(payload[4 + i]);
	}
	if (driver->playerId >= 0 && driver->playerId < MAX_GBAS) {
		driver->otherModes[driver->playerId] = driver->mode;
	}
	driver->stateDirty = true;
	ConditionWake(&driver->cond);
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: state update playerId=%d attached=%d presentMask=%02X",
	     driver->playerId, driver->attached, presentMask);
	MutexUnlock(&driver->mutex);
	_wakeDriver(driver);
	return true;
}

static bool _handleTransferBeginPacket(struct GBASIONetPlayLockstepDriver* driver, const uint8_t* payload, size_t size) {
	bool transferInFlight;
	bool validMode;
	uint32_t sequence;
	int32_t startCycle = 0;
	bool hasStartCycle = false;
	enum GBASIOMode mode;
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
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: ignoring duplicate transfer %u begin while active",
		     (unsigned) sequence);
		MutexUnlock(&driver->mutex);
		return true;
	}
	if (_pendingBeginQueueContainsSequence(driver, sequence)) {
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: ignoring duplicate transfer %u begin while queued",
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

	driver->stateDirty = true;
	ConditionWake(&driver->cond);
	if (transferInFlight) {
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin queued while transfer %u is active",
		     (unsigned) sequence, (unsigned) driver->transferSequence);
	} else {
		if (hasStartCycle) {
			mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin received (mode=%u, attached=%u, startCycle=%08X)",
			     (unsigned) sequence, payload[4], payload[6], (unsigned) (uint32_t) startCycle);
		} else {
			mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin received (mode=%u, attached=%u)",
			     (unsigned) sequence, payload[4], payload[6]);
		}
	}
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer %u begin queued (playerId=%d, depth=%u, sio=%p)",
	     (unsigned) sequence, driver->playerId, (unsigned) driver->pendingBeginCount, (void*) driver->d.p);
	MutexUnlock(&driver->mutex);
	_wakeDriver(driver);
	return true;
}

static bool _handleTransferResultPacket(struct GBASIONetPlayLockstepDriver* driver, const uint8_t* payload, size_t size) {
	struct GBASIONetPlayLockstepTransferResult result;
	int i;
	uint32_t sequence;
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
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: ignoring duplicate transfer result for sequence %u",
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
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: transfer result %u queued while no transfer is active",
		     (unsigned) sequence);
	}
	driver->stateDirty = true;
	ConditionWake(&driver->cond);
	MutexUnlock(&driver->mutex);
	_wakeDriver(driver);
	return true;
}

static bool _handleIncomingPacket(struct GBASIONetPlayLockstepDriver* driver, uint8_t type, const uint8_t* payload, size_t size) {
	bool handled = false;
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: received packet type=%u size=%u", type, (unsigned) size);
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
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: processed packet type=%u successfully", type);
	} else {
		mLOG(GBA_SIO, WARN, "NetPlay lockstep: failed to process packet type=%u", type);
	}
	return handled;
}

static THREAD_ENTRY _readerThread(void* context) {
	struct GBASIONetPlayLockstepDriver* driver = context;
	uint8_t header[8];
	uint8_t payload[MAX_PACKET_SIZE];
	uint32_t size;
	bool stopping = false;
	struct mLogger* logger = NULL;

	MutexLock(&driver->mutex);
	logger = driver->readerLogger;
	MutexUnlock(&driver->mutex);
	if (logger) {
		mLogSetThreadLogger(logger);
	}

	ThreadSetName("NetPlay Relay");
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: relay reader thread running");
	while (true) {
		if (!_recvAll(driver, header, sizeof(header))) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: reader failed to read packet header");
			break;
		}
		size = _read32BE(&header[4]);
		if (size > sizeof(payload)) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: reader got oversized packet (%u)", (unsigned) size);
			break;
		}
		if (size && !_recvAll(driver, payload, size)) {
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
	driver->cycleSyncValid = false;
	driver->cycleSyncOffset = 0;
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
