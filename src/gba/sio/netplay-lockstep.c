/* Copyright (c) 2026 Maik Mursall
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
#include <mgba/internal/gba/sio/netplay-lockstep.h>

#include <mgba/internal/gba/gba.h>
#include <mgba/internal/gba/io.h>

#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DRIVER_ID 0x70746B4E

#define EVENT_ACTIVE_INTERVAL 4096
#define EVENT_SLEEP_INTERVAL 64
#define EVENT_DISCONNECTED_INTERVAL 8192
#define IO_POLL_ACTIVE_MS 1

#define MAX_TOKENS 16
#define MAX_LINE GBA_SIO_NETPLAY_LOCKSTEP_MAX_LINE

enum NetPlayLockstepEventType {
	NP_EV_ATTACH = 0,
	NP_EV_DETACH = 1,
	NP_EV_HARD_SYNC = 2,
	NP_EV_MODE_SET = 3,
	NP_EV_TRANSFER_START = 4,
};

const uint16_t GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT = 6000;
const char GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST[] = "127.0.0.1";

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
static bool GBASIONetPlayLockstepDriverStart(struct GBASIODriver* driver);
static void GBASIONetPlayLockstepDriverFinishMultiplayer(struct GBASIODriver* driver, uint16_t data[4]);
static uint8_t GBASIONetPlayLockstepDriverFinishNormal8(struct GBASIODriver* driver);
static uint32_t GBASIONetPlayLockstepDriverFinishNormal32(struct GBASIODriver* driver);

static void _netPlayEvent(struct mTiming* timing, void* context, uint32_t cyclesLate);

static int _modeEnumToInt(enum GBASIOMode mode);
static enum GBASIOMode _modeIntToEnum(int mode);
static int32_t _now(struct GBASIONetPlayLockstepDriver* net);
static uint32_t _readLocalTransferData(struct GBASIONetPlayLockstepDriver* net, enum GBASIOMode mode);
static void _resyncToTimestampLocked(struct GBASIONetPlayLockstepDriver* net, int32_t timestamp, const char* source);

static void _setDisconnectedLocked(struct GBASIONetPlayLockstepDriver* net);
static void _sleepLocked(struct GBASIONetPlayLockstepDriver* net);
static void _wakeLocked(struct GBASIONetPlayLockstepDriver* net);
static void _setReadyLocked(struct GBASIONetPlayLockstepDriver* net, int playerId, enum GBASIOMode mode);
static void _updateMultiplayerIdentityLocked(struct GBASIONetPlayLockstepDriver* net);
static void _updateReadyStateLocked(struct GBASIONetPlayLockstepDriver* net);

static bool _sendCommandLocked(struct GBASIONetPlayLockstepDriver* net, const char* fmt, ...);
static bool _sendHelloLocked(struct GBASIONetPlayLockstepDriver* net);

static bool _parseInt32(const char* text, int32_t* out);
static bool _parseUint32(const char* text, uint32_t* out);
static int _tokenize(char* line, char* tokens[], int maxTokens);

static THREAD_ENTRY _ioThread(void* context);
static void _joinIoThread(struct GBASIONetPlayLockstepDriver* net);
static bool _queueLineLocked(struct GBASIONetPlayLockstepDriver* net, const char* line, size_t lineLength);
static bool _queueOutgoingLocked(struct GBASIONetPlayLockstepDriver* net, const char* line, size_t lineLength);
static void _flushOutgoingLocked(struct GBASIONetPlayLockstepDriver* net);
static size_t _outQueueDepth(const struct GBASIONetPlayLockstepDriver* net);
static void _drainIncomingLocked(struct GBASIONetPlayLockstepDriver* net);
static void _maybeProcessPendingTransferStartLocked(struct GBASIONetPlayLockstepDriver* net);
static void _completeTransferSoonLocked(struct GBASIONetPlayLockstepDriver* net);

static void _handleLineLocked(struct GBASIONetPlayLockstepDriver* net, char* line);
static void _handleEventLocked(struct GBASIONetPlayLockstepDriver* net, int type, int32_t timestamp, int playerId, int32_t value);

void GBASIONetPlayLockstepDriverCreate(struct GBASIONetPlayLockstepDriver* net, struct mLockstepUser* user) {
	memset(net, 0, sizeof(*net));
	net->d.init = GBASIONetPlayLockstepDriverInit;
	net->d.deinit = GBASIONetPlayLockstepDriverDeinit;
	net->d.reset = GBASIONetPlayLockstepDriverReset;
	net->d.driverId = GBASIONetPlayLockstepDriverId;
	net->d.setMode = GBASIONetPlayLockstepDriverSetMode;
	net->d.handlesMode = GBASIONetPlayLockstepDriverHandlesMode;
	net->d.connectedDevices = GBASIONetPlayLockstepDriverConnectedDevices;
	net->d.deviceId = GBASIONetPlayLockstepDriverDeviceId;
	net->d.writeSIOCNT = GBASIONetPlayLockstepDriverWriteSIOCNT;
	net->d.writeRCNT = GBASIONetPlayLockstepDriverWriteRCNT;
	net->d.start = GBASIONetPlayLockstepDriverStart;
	net->d.finishMultiplayer = GBASIONetPlayLockstepDriverFinishMultiplayer;
	net->d.finishNormal8 = GBASIONetPlayLockstepDriverFinishNormal8;
	net->d.finishNormal32 = GBASIONetPlayLockstepDriverFinishNormal32;

	net->event.context = net;
	net->event.callback = _netPlayEvent;
	net->event.name = "GBA SIO NetPlay Lockstep";
	net->event.priority = 0x80;

	net->socket = INVALID_SOCKET;
	net->user = user;
	net->playerId = 0;
	net->attached = 1;
	net->mode = (enum GBASIOMode) -1;
	net->transferMode = (enum GBASIOMode) -1;
	net->ioThreadActive = false;
	net->ioThreadRunning = false;
	net->pendingTransferStart = false;
	net->pendingTransferStartTimestamp = 0;
	net->pendingTransferFinishCycle = 0;
	net->pendingTransferStartsSeen = 0;
	net->pendingTransferStartsProcessed = 0;
	net->pendingTransferStartsOverwritten = 0;
	net->pendingTransferStartsLate = 0;
	net->lineQueueRead = 0;
	net->lineQueueWrite = 0;
	net->outQueueRead = 0;
	net->outQueueWrite = 0;
	net->outQueueHeadOffset = 0;
	net->cycleOffset = 0;
	net->helloPending = false;

	int i;
	for (i = 0; i < MAX_GBAS; ++i) {
		net->otherModes[i] = (enum GBASIOMode) -1;
		net->multiData[i] = 0xFFFF;
		net->normalData[i] = 0xFFFFFFFF;
	}

	MutexInit(&net->mutex);
}

void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver* net) {
	GBASIONetPlayLockstepDriverDisconnect(net);
	_joinIoThread(net);
	MutexDeinit(&net->mutex);
}

bool GBASIONetPlayLockstepDriverConnect(struct GBASIONetPlayLockstepDriver* net, const char* host, uint16_t port) {
	struct Address address;
	Socket socket;
	GBASIONetPlayLockstepDriverDisconnect(net);

	if (!host || !host[0]) {
		host = GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST;
	}
	if (!port) {
		port = GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT;
	}

	if (SocketResolveHost(host, &address)) {
		mLOG(GBA_SIO, ERROR, "NetPlay lockstep: failed to resolve host '%s'", host);
		return false;
	}

	socket = SocketConnectTCP(port, &address);
	if (SOCKET_FAILED(socket)) {
		mLOG(GBA_SIO, ERROR, "NetPlay lockstep: failed to connect to %s:%u", host, port);
		return false;
	}

	SocketSetBlocking(socket, false);
	SocketSetTCPPush(socket, true);

	MutexLock(&net->mutex);
	if (!SOCKET_FAILED(net->socket)) {
		SocketCloseQuiet(net->socket);
	}
	net->socket = socket;
	net->connected = true;
	net->helloSent = false;
	net->helloPending = false;
	net->asleep = false;
	net->transferActive = false;
	net->dataReceived = false;
	net->pendingTransferStart = false;
	net->pendingTransferStartTimestamp = 0;
	net->pendingTransferFinishCycle = 0;
	net->pendingTransferStartsSeen = 0;
	net->pendingTransferStartsProcessed = 0;
	net->pendingTransferStartsOverwritten = 0;
	net->pendingTransferStartsLate = 0;
	net->playerId = 0;
	net->attached = 1;
	net->rxBufferSize = 0;
	net->lineQueueRead = 0;
	net->lineQueueWrite = 0;
	net->outQueueRead = 0;
	net->outQueueWrite = 0;
	net->outQueueHeadOffset = 0;
	net->cycleOffset = 0;
	net->helloPending = false;
	if (net->d.p) {
		net->mode = net->d.p->mode;
	}
	net->transferMode = net->mode;

	int i;
	for (i = 0; i < MAX_GBAS; ++i) {
		net->otherModes[i] = (enum GBASIOMode) -1;
	}
	net->otherModes[0] = net->mode;

	if (!_sendHelloLocked(net)) {
		_setDisconnectedLocked(net);
		MutexUnlock(&net->mutex);
		return false;
	}

	if (ThreadCreate(&net->ioThread, _ioThread, net)) {
		mLOG(GBA_SIO, ERROR, "NetPlay lockstep: failed to start socket IO thread");
		_setDisconnectedLocked(net);
		MutexUnlock(&net->mutex);
		_joinIoThread(net);
		return false;
	}
	net->ioThreadActive = true;
	net->ioThreadRunning = true;

	if (net->d.p && net->d.p->p) {
		mTimingDeschedule(&net->d.p->p->timing, &net->event);
		mTimingSchedule(&net->d.p->p->timing, &net->event, 1);
	}
	MutexUnlock(&net->mutex);
	return true;
}

bool GBASIONetPlayLockstepDriverConnectDefault(struct GBASIONetPlayLockstepDriver* net) {
	return GBASIONetPlayLockstepDriverConnect(net, GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST, GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT);
}

void GBASIONetPlayLockstepDriverDisconnect(struct GBASIONetPlayLockstepDriver* net) {
	MutexLock(&net->mutex);
	if (net->connected && net->helloSent) {
		_sendCommandLocked(net, "DETACH %" PRId32, _now(net));
		_flushOutgoingLocked(net);
	}
	_setDisconnectedLocked(net);
	MutexUnlock(&net->mutex);
	_joinIoThread(net);
}

bool GBASIONetPlayLockstepDriverIsConnected(const struct GBASIONetPlayLockstepDriver* net) {
	bool connected;
	MutexLock((Mutex*) &net->mutex);
	connected = net->connected;
	MutexUnlock((Mutex*) &net->mutex);
	return connected;
}

static bool GBASIONetPlayLockstepDriverInit(struct GBASIODriver* driver) {
	GBASIONetPlayLockstepDriverReset(driver);
	return true;
}

static void GBASIONetPlayLockstepDriverDeinit(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	if (driver->p && driver->p->p) {
		mTimingDeschedule(&driver->p->p->timing, &net->event);
	}
	GBASIONetPlayLockstepDriverDisconnect(net);
}

static void GBASIONetPlayLockstepDriverReset(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	MutexLock(&net->mutex);
	net->mode = driver->p ? driver->p->mode : net->mode;
	net->transferMode = net->mode;
	net->transferActive = false;
	net->dataReceived = false;
	net->pendingTransferStart = false;
	net->pendingTransferStartTimestamp = 0;
	net->pendingTransferFinishCycle = 0;
	if (!net->connected) {
		net->rxBufferSize = 0;
		net->lineQueueRead = 0;
		net->lineQueueWrite = 0;
		net->outQueueRead = 0;
		net->outQueueWrite = 0;
		net->outQueueHeadOffset = 0;
		net->helloPending = false;
	}

	int i;
	for (i = 0; i < MAX_GBAS; ++i) {
		net->otherModes[i] = (enum GBASIOMode) -1;
	}
	if (net->playerId >= 0 && net->playerId < MAX_GBAS) {
		net->otherModes[net->playerId] = net->mode;
	}
	_updateReadyStateLocked(net);
	if (net->connected && !net->helloSent) {
		_sendHelloLocked(net);
	}
	MutexUnlock(&net->mutex);

	if (driver->p && driver->p->p) {
		mTimingDeschedule(&driver->p->p->timing, &net->event);
		mTimingSchedule(&driver->p->p->timing, &net->event, 1);
	}
}

static uint32_t GBASIONetPlayLockstepDriverId(const struct GBASIODriver* driver) {
	UNUSED(driver);
	return DRIVER_ID;
}

static void GBASIONetPlayLockstepDriverSetMode(struct GBASIODriver* driver, enum GBASIOMode mode) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	MutexLock(&net->mutex);
	if (mode == net->mode) {
		MutexUnlock(&net->mutex);
		return;
	}
	net->mode = mode;
	if (net->playerId >= 0 && net->playerId < MAX_GBAS) {
		net->otherModes[net->playerId] = mode;
	}
	if (net->playerId == 0) {
		net->transferMode = mode;
	}
	_updateReadyStateLocked(net);
	if (net->connected && net->helloSent) {
		if (_sendCommandLocked(net, "SET_MODE %d %" PRId32, _modeEnumToInt(mode), _now(net))) {
			if (net->playerId == 0 && net->attached > 1) {
				_sleepLocked(net);
			}
		}
	}
	MutexUnlock(&net->mutex);
}

static bool GBASIONetPlayLockstepDriverHandlesMode(struct GBASIODriver* driver, enum GBASIOMode mode) {
	UNUSED(driver);
	UNUSED(mode);
	return true;
}

static int GBASIONetPlayLockstepDriverConnectedDevices(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	int attached;
	MutexLock(&net->mutex);
	attached = net->connected ? net->attached : 1;
	MutexUnlock(&net->mutex);
	if (attached < 1) {
		return 0;
	}
	return attached - 1;
}

static int GBASIONetPlayLockstepDriverDeviceId(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	int playerId;
	MutexLock(&net->mutex);
	playerId = net->playerId;
	MutexUnlock(&net->mutex);
	if (playerId < 0 || playerId >= MAX_GBAS) {
		return 0;
	}
	return playerId;
}

static uint16_t GBASIONetPlayLockstepDriverWriteSIOCNT(struct GBASIODriver* driver, uint16_t value) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: SIOCNT <- %04X", value);
	MutexLock(&net->mutex);
	if (net->connected) {
		_drainIncomingLocked(net);
	}
	_updateReadyStateLocked(net);
	MutexUnlock(&net->mutex);
	return value;
}

static uint16_t GBASIONetPlayLockstepDriverWriteRCNT(struct GBASIODriver* driver, uint16_t value) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: RCNT <- %04X", value);
	MutexLock(&net->mutex);
	if (net->connected) {
		_drainIncomingLocked(net);
	}
	_updateReadyStateLocked(net);
	MutexUnlock(&net->mutex);
	return value;
}

static bool GBASIONetPlayLockstepDriverStart(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	bool started = false;
	MutexLock(&net->mutex);
	if (net->connected) {
		_drainIncomingLocked(net);
	}
	if (!net->connected || !net->helloSent) {
		goto out;
	}
	if (net->transferActive) {
		mLOG(GBA_SIO, GAME_ERROR, "Transfer restarted unexpectedly");
		goto out;
	}
	if (net->attached < 2) {
		mLOG(GBA_SIO, DEBUG, "Attempted to start transfer with no secondary players");
		goto out;
	}
	if (net->playerId != 0) {
		mLOG(GBA_SIO, DEBUG, "Secondary player attempted to start transfer");
		goto out;
	}

	int32_t timestamp = _now(net);
	int32_t finishCycle = timestamp + GBASIOTransferCycles(driver->p->mode, driver->p->siocnt, net->attached - 1);
	uint32_t txData = _readLocalTransferData(net, driver->p->mode);

	net->transferMode = driver->p->mode;
	net->transferActive = true;
	net->dataReceived = false;
	started = _sendCommandLocked(net, "START_TRANSFER %" PRId32 " %" PRId32 " %" PRId32,
	                             timestamp, finishCycle, (int32_t) txData);
	if (!started) {
		net->transferActive = false;
		// Never leave SIOMULTI start bit latched if the remote coordinator rejected
		// or could not accept this transfer start.
		_completeTransferSoonLocked(net);
	} else {
		_sleepLocked(net);
	}
out:
	MutexUnlock(&net->mutex);
	return started;
}

static void GBASIONetPlayLockstepDriverFinishMultiplayer(struct GBASIODriver* driver, uint16_t data[4]) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	MutexLock(&net->mutex);
	if (net->connected) {
		_drainIncomingLocked(net);
	}
	if (net->transferMode == GBA_SIO_MULTI) {
		if (!net->dataReceived) {
			mLOG(GBA_SIO, WARN, "MULTI did not receive data. Are we running behind?");
			memset(data, 0xFF, sizeof(uint16_t) * 4);
		} else {
			memcpy(data, net->multiData, sizeof(uint16_t) * 4);
		}
		net->dataReceived = false;
		if (net->playerId == 0 && net->connected && net->helloSent) {
			if (_sendCommandLocked(net, "HARD_SYNC %" PRId32, _now(net))) {
				if (net->attached > 1) {
					_sleepLocked(net);
				}
			}
		}
	}
	MutexUnlock(&net->mutex);
}

static uint8_t GBASIONetPlayLockstepDriverFinishNormal8(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	uint8_t data = 0xFF;
	MutexLock(&net->mutex);
	if (net->connected) {
		_drainIncomingLocked(net);
	}
	if (net->transferMode == GBA_SIO_NORMAL_8) {
		if (net->playerId > 0) {
			if (!net->dataReceived) {
				mLOG(GBA_SIO, WARN, "NORMAL did not receive data. Are we running behind?");
			} else {
				data = net->normalData[net->playerId - 1];
			}
		}
		net->dataReceived = false;
		if (net->playerId == 0 && net->connected && net->helloSent) {
			if (_sendCommandLocked(net, "HARD_SYNC %" PRId32, _now(net))) {
				if (net->attached > 1) {
					_sleepLocked(net);
				}
			}
		}
	}
	MutexUnlock(&net->mutex);
	return data;
}

static uint32_t GBASIONetPlayLockstepDriverFinishNormal32(struct GBASIODriver* driver) {
	struct GBASIONetPlayLockstepDriver* net = (struct GBASIONetPlayLockstepDriver*) driver;
	uint32_t data = 0xFFFFFFFF;
	MutexLock(&net->mutex);
	if (net->connected) {
		_drainIncomingLocked(net);
	}
	if (net->transferMode == GBA_SIO_NORMAL_32) {
		if (net->playerId > 0) {
			if (!net->dataReceived) {
				mLOG(GBA_SIO, WARN, "Did not receive data. Are we running behind?");
			} else {
				data = net->normalData[net->playerId - 1];
			}
		}
		net->dataReceived = false;
		if (net->playerId == 0 && net->connected && net->helloSent) {
			if (_sendCommandLocked(net, "HARD_SYNC %" PRId32, _now(net))) {
				if (net->attached > 1) {
					_sleepLocked(net);
				}
			}
		}
	}
	MutexUnlock(&net->mutex);
	return data;
}

static void _netPlayEvent(struct mTiming* timing, void* context, uint32_t cyclesLate) {
	UNUSED(cyclesLate);
	struct GBASIONetPlayLockstepDriver* net = context;
	uint32_t next = EVENT_DISCONNECTED_INTERVAL;
	bool joinIoThread = false;
	MutexLock(&net->mutex);
	if (net->connected) {
		if (!net->helloSent) {
			_sendHelloLocked(net);
		}
		if (net->connected && net->helloSent && net->playerId == 0 && net->attached > 1 && !net->asleep) {
			// Avoid unbounded TICK buildup if the socket writer falls behind.
			if (_outQueueDepth(net) < (GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE / 2)) {
				_sendCommandLocked(net, "TICK %" PRId32, _now(net));
			}
		}
		if (net->connected) {
			_drainIncomingLocked(net);
		}
		if (net->connected) {
			_maybeProcessPendingTransferStartLocked(net);
		}
	}
	if (net->connected) {
		next = net->asleep ? EVENT_SLEEP_INTERVAL : EVENT_ACTIVE_INTERVAL;
		if (net->pendingTransferStart) {
			int32_t untilTransferStart = net->pendingTransferStartTimestamp - _now(net);
			if (untilTransferStart < 1) {
				untilTransferStart = 1;
			}
			if ((uint32_t) untilTransferStart < next) {
				next = (uint32_t) untilTransferStart;
			}
		}
	}
	if (!net->connected && net->ioThreadActive && !net->ioThreadRunning) {
		joinIoThread = true;
	}
	MutexUnlock(&net->mutex);
	if (joinIoThread) {
		_joinIoThread(net);
	}
	mTimingSchedule(timing, &net->event, next);
}

static int _modeEnumToInt(enum GBASIOMode mode) {
	switch ((int) mode) {
	case -1:
	default:
		return 0;
	case GBA_SIO_MULTI:
		return 1;
	case GBA_SIO_NORMAL_8:
		return 2;
	case GBA_SIO_NORMAL_32:
		return 3;
	case GBA_SIO_GPIO:
		return 4;
	case GBA_SIO_UART:
		return 5;
	case GBA_SIO_JOYBUS:
		return 6;
	}
}

static enum GBASIOMode _modeIntToEnum(int mode) {
	const enum GBASIOMode modes[8] = {
		-1, GBA_SIO_MULTI, GBA_SIO_NORMAL_8, GBA_SIO_NORMAL_32, GBA_SIO_GPIO, GBA_SIO_UART, GBA_SIO_JOYBUS, -1
	};
	return modes[mode & 7];
}

static int32_t _now(struct GBASIONetPlayLockstepDriver* net) {
	if (!net->d.p || !net->d.p->p) {
		return 0;
	}
	return mTimingCurrentTime(&net->d.p->p->timing) - net->cycleOffset;
}

static void _resyncToTimestampLocked(struct GBASIONetPlayLockstepDriver* net, int32_t timestamp, const char* source) {
	if (!net->d.p || !net->d.p->p) {
		return;
	}

	int32_t now = _now(net);
	int32_t skew = now - timestamp;
	if (skew >= -0x2000 && skew <= 0x2000) {
		return;
	}

	net->cycleOffset = mTimingCurrentTime(&net->d.p->p->timing) - timestamp;
	++net->pendingTransferStartsLate;
	mLOG(GBA_SIO, WARN,
	     "NetPlay lockstep: clock resync source=%s player=%d skew=%" PRId32 " newOffset=%" PRId32 " stats(seen=%" PRIu32 ",processed=%" PRIu32 ",overwritten=%" PRIu32 ",resync=%" PRIu32 ")",
	     source,
	     net->playerId,
	     skew,
	     net->cycleOffset,
	     net->pendingTransferStartsSeen,
	     net->pendingTransferStartsProcessed,
	     net->pendingTransferStartsOverwritten,
	     net->pendingTransferStartsLate);
}

static uint32_t _readLocalTransferData(struct GBASIONetPlayLockstepDriver* net, enum GBASIOMode mode) {
	struct GBASIO* sio = net->d.p;
	if (!sio || !sio->p) {
		return 0xFFFFFFFF;
	}
	switch (mode) {
	case GBA_SIO_MULTI:
		return sio->p->memory.io[GBA_REG(SIOMLT_SEND)];
	case GBA_SIO_NORMAL_8:
		return sio->p->memory.io[GBA_REG(SIODATA8)];
	case GBA_SIO_NORMAL_32:
		return sio->p->memory.io[GBA_REG(SIODATA32_LO)] | sio->p->memory.io[GBA_REG(SIODATA32_HI)] << 16;
	default:
		return 0xFFFFFFFF;
	}
}

static void _setDisconnectedLocked(struct GBASIONetPlayLockstepDriver* net) {
	int oldPlayerId = net->playerId;
	uint32_t pendingSeen = net->pendingTransferStartsSeen;
	uint32_t pendingProcessed = net->pendingTransferStartsProcessed;
	uint32_t pendingOverwritten = net->pendingTransferStartsOverwritten;
	uint32_t pendingLate = net->pendingTransferStartsLate;
	if (!SOCKET_FAILED(net->socket)) {
		SocketCloseQuiet(net->socket);
	}
	net->socket = INVALID_SOCKET;
	net->connected = false;
	net->helloSent = false;
	net->helloPending = false;
	net->transferActive = false;
	net->dataReceived = false;
	net->pendingTransferStart = false;
	net->pendingTransferStartTimestamp = 0;
	net->pendingTransferFinishCycle = 0;
	net->pendingTransferStartsSeen = 0;
	net->pendingTransferStartsProcessed = 0;
	net->pendingTransferStartsOverwritten = 0;
	net->pendingTransferStartsLate = 0;
	net->attached = 1;
	net->playerId = 0;
	net->cycleOffset = 0;
	net->rxBufferSize = 0;
	net->lineQueueRead = 0;
	net->lineQueueWrite = 0;

	int i;
	for (i = 0; i < MAX_GBAS; ++i) {
		net->otherModes[i] = (enum GBASIOMode) -1;
	}
	net->otherModes[0] = net->mode;
	if (oldPlayerId != 0 && net->user && net->user->playerIdChanged) {
		net->user->playerIdChanged(net->user, 0);
	}
	if (net->asleep) {
		_wakeLocked(net);
	}
	_updateReadyStateLocked(net);
	if ((pendingSeen || pendingProcessed || pendingOverwritten || pendingLate) && pendingSeen != pendingProcessed) {
		mLOG(GBA_SIO, WARN,
		     "NetPlay lockstep: disconnect with pending transfer mismatch seen=%" PRIu32 " processed=%" PRIu32 " overwritten=%" PRIu32 " late=%" PRIu32,
		     pendingSeen,
		     pendingProcessed,
		     pendingOverwritten,
		     pendingLate);
	}
}

static void _sleepLocked(struct GBASIONetPlayLockstepDriver* net) {
	if (net->asleep) {
		return;
	}
	net->asleep = true;
	if (net->user && net->user->sleep) {
		net->user->sleep(net->user);
	}
	if (net->d.p && net->d.p->p) {
		net->d.p->p->cpu->nextEvent = 0;
		GBAInterrupt(net->d.p->p);
	}
}

static void _wakeLocked(struct GBASIONetPlayLockstepDriver* net) {
	if (!net->asleep) {
		return;
	}
	net->asleep = false;
	if (net->user && net->user->wake) {
		net->user->wake(net->user);
	}
}

static void _setReadyLocked(struct GBASIONetPlayLockstepDriver* net, int playerId, enum GBASIOMode mode) {
	if (playerId < 0 || playerId >= MAX_GBAS) {
		return;
	}
	net->otherModes[playerId] = mode;
	bool ready = true;
	int i;
	for (i = 0; ready && i < net->attached && i < MAX_GBAS; ++i) {
		ready = net->otherModes[i] == net->mode;
	}
	if (net->mode == GBA_SIO_MULTI && net->d.p) {
		struct GBASIO* sio = net->d.p;
		sio->siocnt = GBASIOMultiplayerSetReady(sio->siocnt, ready);
		sio->rcnt = GBASIORegisterRCNTSetSd(sio->rcnt, ready);
	}
}

static void _updateMultiplayerIdentityLocked(struct GBASIONetPlayLockstepDriver* net) {
	if (net->mode != GBA_SIO_MULTI || !net->d.p) {
		return;
	}
	int id = 0;
	if (net->playerId >= 0 && net->playerId < MAX_GBAS) {
		id = net->playerId;
	}
	net->d.p->siocnt = GBASIOMultiplayerSetId(net->d.p->siocnt, id);
	net->d.p->siocnt = GBASIOMultiplayerSetSlave(net->d.p->siocnt, id || net->attached < 2);
	net->d.p->rcnt = GBASIORegisterRCNTSetSi(net->d.p->rcnt, !!id);
}

static void _updateReadyStateLocked(struct GBASIONetPlayLockstepDriver* net) {
	if (net->playerId >= 0 && net->playerId < MAX_GBAS) {
		_setReadyLocked(net, net->playerId, net->mode);
	}
	_updateMultiplayerIdentityLocked(net);
}

static bool _queueOutgoingLocked(struct GBASIONetPlayLockstepDriver* net, const char* line, size_t lineLength) {
	size_t next = net->outQueueWrite + 1;
	if (next >= GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE) {
		next = 0;
	}
	if (next == net->outQueueRead) {
		_flushOutgoingLocked(net);
		if (!net->connected || SOCKET_FAILED(net->socket)) {
			return false;
		}
		next = net->outQueueWrite + 1;
		if (next >= GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE) {
			next = 0;
		}
		if (next == net->outQueueRead) {
			mLOG(GBA_SIO, WARN, "NetPlay lockstep: outgoing line queue full, dropping command");
			return false;
		}
	}

	if (lineLength >= MAX_LINE) {
		lineLength = MAX_LINE - 1;
	}
	char* slot = net->outQueue[net->outQueueWrite];
	memcpy(slot, line, lineLength);
	slot[lineLength] = '\0';
	net->outQueueWrite = next;
	return true;
}

static size_t _outQueueDepth(const struct GBASIONetPlayLockstepDriver* net) {
	if (net->outQueueWrite >= net->outQueueRead) {
		return net->outQueueWrite - net->outQueueRead;
	}
	return GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE - (net->outQueueRead - net->outQueueWrite);
}

static void _flushOutgoingLocked(struct GBASIONetPlayLockstepDriver* net) {
	while (net->connected && net->outQueueRead != net->outQueueWrite) {
		size_t index = net->outQueueRead;
		size_t lineLength = strlen(net->outQueue[index]);
		if (net->outQueueHeadOffset >= lineLength) {
			net->outQueueHeadOffset = 0;
			net->outQueueRead = (index + 1) % GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE;
			continue;
		}

		Socket socket = net->socket;
		const uint8_t* chunk = (const uint8_t*) &net->outQueue[index][net->outQueueHeadOffset];
		size_t remaining = lineLength - net->outQueueHeadOffset;

		MutexUnlock(&net->mutex);
		ssize_t written = SocketSend(socket, chunk, remaining);
		bool wouldBlock = written < 0 && SocketWouldBlock();
		MutexLock(&net->mutex);

		if (!net->connected || SOCKET_FAILED(net->socket) || net->socket != socket) {
			if (!net->connected) {
				return;
			}
			continue;
		}
		if (written > 0) {
			net->outQueueHeadOffset += (size_t) written;
			continue;
		}
		if (!written) {
			_setDisconnectedLocked(net);
			return;
		}
		if (wouldBlock) {
			return;
		}
		_setDisconnectedLocked(net);
		return;
	}
}

static bool _sendCommandLocked(struct GBASIONetPlayLockstepDriver* net, const char* fmt, ...) {
	char line[MAX_LINE];
	va_list args;
	va_start(args, fmt);
	int length = vsnprintf(line, sizeof(line), fmt, args);
	va_end(args);
	if (length < 0 || length >= (int) sizeof(line) - 1) {
		return false;
	}
	line[length++] = '\n';
	line[length] = '\0';
	if (!net->connected || SOCKET_FAILED(net->socket)) {
		return false;
	}
	return _queueOutgoingLocked(net, line, (size_t) length);
}

static bool _sendHelloLocked(struct GBASIONetPlayLockstepDriver* net) {
	if (!net->connected || net->helloSent || net->helloPending) {
		return net->connected;
	}
	int requestedId = MAX_GBAS - 1;
	if (net->user && net->user->requestedId) {
		requestedId = net->user->requestedId(net->user);
	}
	if (_sendCommandLocked(net, "HELLO %d %d %" PRId32,
	                       _modeEnumToInt(net->mode),
	                       requestedId,
	                       _now(net))) {
		net->helloPending = true;
		_flushOutgoingLocked(net);
		return net->connected;
	}
	return false;
}

static void _joinIoThread(struct GBASIONetPlayLockstepDriver* net) {
	Thread thread;
	bool join = false;

	MutexLock(&net->mutex);
	if (net->ioThreadActive) {
		thread = net->ioThread;
		net->ioThreadActive = false;
		join = true;
	}
	MutexUnlock(&net->mutex);

	if (join) {
		ThreadJoin(&thread);
	}
}

static bool _queueLineLocked(struct GBASIONetPlayLockstepDriver* net, const char* line, size_t lineLength) {
	size_t next = net->lineQueueWrite + 1;
	if (next >= GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE) {
		next = 0;
	}
	if (next == net->lineQueueRead) {
		mLOG(GBA_SIO, ERROR, "NetPlay lockstep: incoming line queue overflow");
		_setDisconnectedLocked(net);
		return false;
	}

	if (lineLength >= MAX_LINE) {
		lineLength = MAX_LINE - 1;
	}
	char* slot = net->lineQueue[net->lineQueueWrite];
	memcpy(slot, line, lineLength);
	slot[lineLength] = '\0';
	net->lineQueueWrite = next;

	// Wake the core thread so queued network data can be handled immediately.
	if (net->asleep) {
		_wakeLocked(net);
	}
	return true;
}

static void _drainIncomingLocked(struct GBASIONetPlayLockstepDriver* net) {
	while (net->connected && net->lineQueueRead != net->lineQueueWrite) {
		char line[MAX_LINE];
		size_t index = net->lineQueueRead;
		net->lineQueueRead = (index + 1) % GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE;

		strncpy(line, net->lineQueue[index], sizeof(line));
		line[sizeof(line) - 1] = '\0';

		_handleLineLocked(net, line);
	}
	_maybeProcessPendingTransferStartLocked(net);
}

static void _maybeProcessPendingTransferStartLocked(struct GBASIONetPlayLockstepDriver* net) {
	if (!net->pendingTransferStart || !net->connected || !net->helloSent || net->playerId <= 0 || !net->d.p || !net->d.p->p) {
		return;
	}

	int32_t now = _now(net);
	int32_t lag = now - net->pendingTransferStartTimestamp;
	if (lag < 0) {
		return;
	}
	if (lag > 0x2000) {
		_resyncToTimestampLocked(net, net->pendingTransferStartTimestamp, "TRANSFER_START");
		now = _now(net);
	}

	uint32_t txData = _readLocalTransferData(net, net->transferMode);
	int32_t delay = net->pendingTransferFinishCycle - now;
	if (delay < 1) {
		int32_t duration = net->pendingTransferFinishCycle - net->pendingTransferStartTimestamp;
		if (duration < 1) {
			duration = GBASIOTransferCycles(net->transferMode, net->d.p->siocnt, net->attached - 1);
		}
		if (duration < 1) {
			duration = 1;
		}
		delay = duration;
	}

	_sendCommandLocked(net, "SUBMIT_DATA %" PRId32, (int32_t) txData);

	net->d.p->siocnt |= 0x80;
	mTimingDeschedule(&net->d.p->p->timing, &net->d.p->completeEvent);
	mTimingSchedule(&net->d.p->p->timing, &net->d.p->completeEvent, delay);
	_sendCommandLocked(net, "ACK");

	++net->pendingTransferStartsProcessed;
	net->pendingTransferStart = false;
	net->pendingTransferStartTimestamp = 0;
	net->pendingTransferFinishCycle = 0;
	if (!(net->pendingTransferStartsProcessed & 0xFF)) {
		mLOG(GBA_SIO, DEBUG,
		     "NetPlay lockstep: pending transfer stats seen=%" PRIu32 " processed=%" PRIu32 " overwritten=%" PRIu32 " late=%" PRIu32,
		     net->pendingTransferStartsSeen,
		     net->pendingTransferStartsProcessed,
		     net->pendingTransferStartsOverwritten,
		     net->pendingTransferStartsLate);
	}
}

static void _completeTransferSoonLocked(struct GBASIONetPlayLockstepDriver* net) {
	struct GBASIO* sio = net->d.p;
	if (!sio || !sio->p || !(sio->siocnt & 0x80)) {
		return;
	}

	mTimingDeschedule(&sio->p->timing, &sio->completeEvent);
	mTimingSchedule(&sio->p->timing, &sio->completeEvent, 1);
}

THREAD_ENTRY _ioThread(void* context) {
	struct GBASIONetPlayLockstepDriver* net = context;
	uint8_t input[512];

	ThreadSetName("NetPlay Socket");

	while (true) {
		Socket r;
		int pollTimeout;

		MutexLock(&net->mutex);
		if (!net->connected || SOCKET_FAILED(net->socket)) {
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}
		_flushOutgoingLocked(net);
		if (!net->connected || SOCKET_FAILED(net->socket)) {
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}
		r = net->socket;
		pollTimeout = IO_POLL_ACTIVE_MS;
		MutexUnlock(&net->mutex);

		int poll = SocketPoll(1, &r, NULL, NULL, pollTimeout);
		if (poll <= 0) {
			continue;
		}
		if (SOCKET_FAILED(r)) {
			MutexLock(&net->mutex);
			_setDisconnectedLocked(net);
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}

		ssize_t read = SocketRecv(r, input, sizeof(input));
		if (!read) {
			MutexLock(&net->mutex);
			_setDisconnectedLocked(net);
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}
		if (read < 0) {
			if (SocketWouldBlock()) {
				continue;
			}
			MutexLock(&net->mutex);
			_setDisconnectedLocked(net);
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}

		MutexLock(&net->mutex);
		if (!net->connected || SOCKET_FAILED(net->socket) || net->socket != r) {
			MutexUnlock(&net->mutex);
			continue;
		}
		if (net->rxBufferSize + read > sizeof(net->rxBuffer)) {
			mLOG(GBA_SIO, ERROR, "NetPlay lockstep: receive buffer overflow");
			_setDisconnectedLocked(net);
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}
		memcpy(&net->rxBuffer[net->rxBufferSize], input, read);
		net->rxBufferSize += read;

		while (net->connected) {
			uint8_t* newline = memchr(net->rxBuffer, '\n', net->rxBufferSize);
			if (!newline) {
				break;
			}
			size_t lineLength = newline - net->rxBuffer;
			size_t consumed = lineLength + 1;
			char line[MAX_LINE];

			if (lineLength && net->rxBuffer[lineLength - 1] == '\r') {
				--lineLength;
			}
			if (lineLength >= sizeof(line)) {
				lineLength = sizeof(line) - 1;
			}
			memcpy(line, net->rxBuffer, lineLength);
			line[lineLength] = '\0';

			memmove(net->rxBuffer, &net->rxBuffer[consumed], net->rxBufferSize - consumed);
			net->rxBufferSize -= consumed;

			if (!_queueLineLocked(net, line, lineLength)) {
				break;
			}
		}

		if (!net->connected) {
			net->ioThreadRunning = false;
			MutexUnlock(&net->mutex);
			break;
		}
		MutexUnlock(&net->mutex);
	}

	THREAD_EXIT(0);
}

static bool _parseInt32(const char* text, int32_t* out) {
	char* end = NULL;
	long value = strtol(text, &end, 10);
	if (!text[0] || end == text || *end) {
		return false;
	}
	if (value < INT32_MIN || value > INT32_MAX) {
		return false;
	}
	*out = value;
	return true;
}

static bool _parseUint32(const char* text, uint32_t* out) {
	char* end = NULL;
	unsigned long long value;
	if (!text[0] || text[0] == '-') {
		return false;
	}
	value = strtoull(text, &end, 10);
	if (end == text || *end || value > UINT32_MAX) {
		return false;
	}
	*out = value;
	return true;
}

static int _tokenize(char* line, char* tokens[], int maxTokens) {
	int count = 0;
	char* cursor = line;
	while (*cursor && count < maxTokens) {
		while (*cursor && isspace((unsigned char) *cursor)) {
			++cursor;
		}
		if (!*cursor) {
			break;
		}
		tokens[count++] = cursor;
		while (*cursor && !isspace((unsigned char) *cursor)) {
			++cursor;
		}
		if (!*cursor) {
			break;
		}
		*cursor++ = '\0';
	}
	return count;
}

static void _handleEventLocked(struct GBASIONetPlayLockstepDriver* net, int type, int32_t timestamp, int playerId, int32_t value) {
	switch (type) {
	case NP_EV_ATTACH:
		_setReadyLocked(net, playerId, -1);
		if (net->playerId == 0 && net->d.p) {
			net->d.p->siocnt = GBASIOMultiplayerClearSlave(net->d.p->siocnt);
		}
		break;
	case NP_EV_DETACH:
		_setReadyLocked(net, playerId, -1);
		_setReadyLocked(net, net->playerId, net->mode);
		_updateMultiplayerIdentityLocked(net);
		break;
	case NP_EV_HARD_SYNC:
		_resyncToTimestampLocked(net, timestamp, "HARD_SYNC");
		if (net->playerId != 0 && net->connected && net->helloSent) {
			_sendCommandLocked(net, "ACK");
		}
		break;
	case NP_EV_MODE_SET:
	{
		enum GBASIOMode mode = _modeIntToEnum(value);
		if (net->transferActive && net->mode != mode) {
			net->transferActive = false;
			net->dataReceived = false;
		}
		_setReadyLocked(net, playerId, mode);
		if (playerId == 0) {
			_resyncToTimestampLocked(net, timestamp, "MODE_SET");
			net->transferMode = mode;
		}
		if (playerId == 0 && net->playerId != 0 && net->connected && net->helloSent) {
			_sendCommandLocked(net, "ACK");
		}
		break;
	}
	case NP_EV_TRANSFER_START:
		if (net->playerId > 0 && net->connected && net->helloSent && net->d.p && net->d.p->p) {
			if (net->pendingTransferStart) {
				++net->pendingTransferStartsOverwritten;
				mLOG(GBA_SIO, WARN,
				     "NetPlay lockstep: pending transfer overwritten player=%d oldStart=%" PRId32 " oldFinish=%" PRId32 " newStart=%" PRId32 " newFinish=%" PRId32 " seen=%" PRIu32 " processed=%" PRIu32 " overwritten=%" PRIu32,
				     net->playerId,
				     net->pendingTransferStartTimestamp,
				     net->pendingTransferFinishCycle,
				     timestamp,
				     value,
				     net->pendingTransferStartsSeen,
				     net->pendingTransferStartsProcessed,
				     net->pendingTransferStartsOverwritten);
			}
			++net->pendingTransferStartsSeen;
			_resyncToTimestampLocked(net, timestamp, "TRANSFER_START_EVENT");
			net->pendingTransferStart = true;
			net->pendingTransferStartTimestamp = timestamp;
			net->pendingTransferFinishCycle = value;
			_maybeProcessPendingTransferStartLocked(net);
		}
		break;
	}
}

static void _handleLineLocked(struct GBASIONetPlayLockstepDriver* net, char* line) {
	char* tokens[MAX_TOKENS];
	int nTokens = _tokenize(line, tokens, MAX_TOKENS);
	if (!nTokens) {
		return;
	}

	if (!strcmp(tokens[0], "WELCOME")) {
		int32_t playerId;
		int32_t attached;
		int32_t transferMode;
		int32_t cycle;
		int oldPlayerId;
		if (nTokens < 7
		    || !_parseInt32(tokens[2], &playerId)
		    || !_parseInt32(tokens[3], &attached)
		    || !_parseInt32(tokens[4], &transferMode)
		    || !_parseInt32(tokens[5], &cycle)) {
			return;
		}

		oldPlayerId = net->playerId;
		if (net->d.p && net->d.p->p) {
			net->cycleOffset = mTimingCurrentTime(&net->d.p->p->timing) - cycle;
		}
		mLOG(GBA_SIO, DEBUG, "NetPlay lockstep: WELCOME player=%" PRId32 " attached=%" PRId32 " transferMode=%" PRId32 " cycle=%" PRId32 " offset=%" PRId32,
		     playerId, attached, transferMode, cycle, net->cycleOffset);
		net->helloPending = false;
		net->helloSent = true;
		net->playerId = playerId;
		net->attached = attached;
		net->transferMode = _modeIntToEnum(transferMode);
		if (net->playerId < 0 || net->playerId >= MAX_GBAS) {
			net->playerId = 0;
		}
		if (net->attached < 1) {
			net->attached = 1;
		} else if (net->attached > MAX_GBAS) {
			net->attached = MAX_GBAS;
		}
		int i;
		for (i = 0; i < MAX_GBAS; ++i) {
			net->otherModes[i] = (enum GBASIOMode) -1;
		}
		net->otherModes[net->playerId] = net->mode;
		if (net->attached > 1) {
			net->otherModes[0] = net->transferMode;
		}
		_updateReadyStateLocked(net);
		if (oldPlayerId != net->playerId && net->user && net->user->playerIdChanged) {
			net->user->playerIdChanged(net->user, net->playerId);
		}
		return;
	}

	if (!strcmp(tokens[0], "PLAYER_ID")) {
		int32_t playerId;
		int oldPlayerId;
		if (nTokens < 2 || !_parseInt32(tokens[1], &playerId)) {
			return;
		}
		oldPlayerId = net->playerId;
		net->playerId = playerId;
		if (net->playerId < 0 || net->playerId >= MAX_GBAS) {
			net->playerId = 0;
		}
		_updateReadyStateLocked(net);
		if (oldPlayerId != net->playerId && net->user && net->user->playerIdChanged) {
			net->user->playerIdChanged(net->user, net->playerId);
		}
		return;
	}

	if (!strcmp(tokens[0], "ATTACHED")) {
		int32_t attached;
		if (nTokens < 2 || !_parseInt32(tokens[1], &attached)) {
			return;
		}
		net->attached = attached;
		if (net->attached < 1) {
			net->attached = 1;
		} else if (net->attached > MAX_GBAS) {
			net->attached = MAX_GBAS;
		}
		_updateReadyStateLocked(net);
		return;
	}

	if (!strcmp(tokens[0], "EVENT")) {
		int32_t type;
		int32_t timestamp;
		int32_t playerId;
		int32_t value;
		if (nTokens < 5
		    || !_parseInt32(tokens[1], &type)
		    || !_parseInt32(tokens[2], &timestamp)
		    || !_parseInt32(tokens[3], &playerId)
		    || !_parseInt32(tokens[4], &value)) {
			return;
		}
		_handleEventLocked(net, type, timestamp, playerId, value);
		return;
	}

	if (!strcmp(tokens[0], "WAKE")) {
		_wakeLocked(net);
		return;
	}

	if (!strcmp(tokens[0], "SLEEP")) {
		_sleepLocked(net);
		return;
	}

	if (!strcmp(tokens[0], "OK")) {
		return;
	}

	if (!strcmp(tokens[0], "ERR")) {
		const char* reason = nTokens >= 2 ? tokens[1] : "unknown";
		mLOG(GBA_SIO, WARN, "NetPlay lockstep server error: %s", reason);
		if (!strcmp(reason, "not_attached_send_HELLO_first")) {
			net->helloSent = false;
			net->helloPending = false;
			net->transferActive = false;
			net->dataReceived = false;
			_sendHelloLocked(net);
			return;
		}
		if (!strcmp(reason, "transfer_already_active")
		    || !strcmp(reason, "no_secondary_players")
		    || !strcmp(reason, "only_primary_can_start")
		    || !strcmp(reason, "wait_in_progress")
		    || !strcmp(reason, "desync_waiting_not_empty")
		    || !strcmp(reason, "desync_primary_asleep")
		    || !strcmp(reason, "desync_non_primary_wait")) {
			net->transferActive = false;
			net->dataReceived = false;
			net->pendingTransferStart = false;
			net->pendingTransferStartTimestamp = 0;
			net->pendingTransferFinishCycle = 0;
			_completeTransferSoonLocked(net);
			if (!strcmp(reason, "wait_in_progress")) {
				_sleepLocked(net);
			}
		}
		return;
	}

	if (!strcmp(tokens[0], "TRANSFER_DONE")) {
		int32_t mode;
		uint32_t values[8];
		if (nTokens < 10 || !_parseInt32(tokens[1], &mode)) {
			return;
		}
		int i;
		for (i = 0; i < 8; ++i) {
			if (!_parseUint32(tokens[i + 2], &values[i])) {
				return;
			}
		}
		mLOG(GBA_SIO, DEBUG,
		     "NetPlay lockstep: parsed TRANSFER_DONE mode=%d multi=%04X,%04X,%04X,%04X normal=%08X,%08X,%08X,%08X",
		     mode,
		     values[0] & 0xFFFF,
		     values[1] & 0xFFFF,
		     values[2] & 0xFFFF,
		     values[3] & 0xFFFF,
		     values[4],
		     values[5],
		     values[6],
		     values[7]);
		net->transferMode = _modeIntToEnum(mode);
		for (i = 0; i < MAX_GBAS; ++i) {
			net->multiData[i] = values[i];
			net->normalData[i] = values[i + MAX_GBAS];
		}
		net->dataReceived = true;
		net->transferActive = false;
		return;
	}
}
