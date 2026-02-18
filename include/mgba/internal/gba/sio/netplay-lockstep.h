/* Copyright (c) 2013-2024 Jeffrey Pfau
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
#ifndef GBA_SIO_NETPLAY_LOCKSTEP_H
#define GBA_SIO_NETPLAY_LOCKSTEP_H

#include <mgba-util/common.h>

CXX_GUARD_START

#include <mgba/core/lockstep.h>
#include <mgba/core/timing.h>
#include <mgba/internal/gba/sio.h>

#include <mgba-util/socket.h>
#include <mgba-util/threading.h>

extern const uint16_t GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_PORT;
extern const char GBA_SIO_NETPLAY_LOCKSTEP_DEFAULT_HOST[];

#define NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE 64
#define NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE 64
#define NETPLAY_LOCKSTEP_SYNC_QUEUE_SIZE 64
#define NETPLAY_LOCKSTEP_ACK_QUEUE_SIZE 64
#define NETPLAY_LOCKSTEP_OUTBOUND_QUEUE_SIZE 128
#define NETPLAY_LOCKSTEP_OUTBOUND_MAX_PAYLOAD 20
#define NETPLAY_LOCKSTEP_MULTI_WRITE_HISTORY_SIZE 64

struct GBASIONetPlayLockstepTransferResult {
	uint32_t sequence;
	enum GBASIOMode mode;
	int attached;
	uint16_t multiData[MAX_GBAS];
	uint32_t normalData[MAX_GBAS];
};

struct GBASIONetPlayLockstepHardSyncDone {
	uint32_t sequence;
};

struct GBASIONetPlayLockstepHardSyncAck {
	uint32_t sequence;
};

struct GBASIONetPlayLockstepOutboundPacket {
	uint8_t type;
	uint8_t size;
	uint8_t payload[NETPLAY_LOCKSTEP_OUTBOUND_MAX_PAYLOAD];
};

struct GBASIONetPlayLockstepPendingBegin {
	uint32_t sequence;
	enum GBASIOMode mode;
	uint8_t attached;
	uint16_t siocnt;
	int32_t startCycle;
	uint8_t hasStartCycle;
};

struct GBASIONetPlayLockstepDriver {
	struct GBASIODriver d;
	struct mTimingEvent event;
	struct mLockstepUser* user;

	Socket socket;
#ifndef DISABLE_THREADING
	Thread thread;
	Mutex mutex;
	Mutex sendMutex;
	Condition cond;
	bool threadRunning;
	struct mLogger* readerLogger;
#endif

	bool connected;
	bool stopping;
	bool asleep;
	uint32_t wakeGeneration;
	uint32_t clientIdleEvents;
	bool waitingForTransfer;
	bool transferActive;
	bool waitingForHardSync;
	uint32_t hardSyncSequence;
	uint32_t transferSequence;
	bool deferredMultiplayerResultValid;
	struct GBASIONetPlayLockstepTransferResult deferredMultiplayerResult;
	bool cycleSyncValid;
	int32_t cycleSyncOffset;
	uint32_t cycleSyncSequence;
	uint32_t multiSendWriteGeneration;
	uint32_t normal8WriteGeneration;
	uint32_t normal32WriteGeneration;
	uint16_t multiSendWriteHistoryValue[NETPLAY_LOCKSTEP_MULTI_WRITE_HISTORY_SIZE];
	int32_t multiSendWriteHistoryCycle[NETPLAY_LOCKSTEP_MULTI_WRITE_HISTORY_SIZE];
	uint32_t multiSendWriteHistoryGeneration[NETPLAY_LOCKSTEP_MULTI_WRITE_HISTORY_SIZE];
	uint8_t multiSendWriteHistoryWrite;
	uint8_t multiSendWriteHistoryCount;
	uint32_t multiSendLastSentGeneration;
	uint32_t normal8LastSentGeneration;
	uint32_t normal32LastSentGeneration;
	bool freshnessWaitActive;
	bool freshnessWaitLogged;
	uint32_t freshnessWaitSequence;
	enum GBASIOMode freshnessWaitMode;
	uint32_t freshnessWaitBaselineGeneration;
	int32_t freshnessWaitStartCycle;
	bool multiSameGenerationFastPathActive;
	uint32_t multiSameGenerationFastPathGeneration;

	int playerId;
	int attached;
	bool present[MAX_GBAS];
	enum GBASIOMode otherModes[MAX_GBAS];
	enum GBASIOMode mode;

	bool stateDirty;
	bool playerIdChanged;
	bool pendingDisconnect;

	struct GBASIONetPlayLockstepPendingBegin pendingBegins[NETPLAY_LOCKSTEP_BEGIN_QUEUE_SIZE];
	uint8_t pendingBeginRead;
	uint8_t pendingBeginWrite;
	uint8_t pendingBeginCount;

	struct GBASIONetPlayLockstepTransferResult pendingResults[NETPLAY_LOCKSTEP_RESULT_QUEUE_SIZE];
	uint8_t pendingResultRead;
	uint8_t pendingResultWrite;
	uint8_t pendingResultCount;

	struct GBASIONetPlayLockstepHardSyncDone pendingSyncs[NETPLAY_LOCKSTEP_SYNC_QUEUE_SIZE];
	uint8_t pendingSyncRead;
	uint8_t pendingSyncWrite;
	uint8_t pendingSyncCount;

	struct GBASIONetPlayLockstepHardSyncAck pendingAcks[NETPLAY_LOCKSTEP_ACK_QUEUE_SIZE];
	uint8_t pendingAckRead;
	uint8_t pendingAckWrite;
	uint8_t pendingAckCount;

	struct GBASIONetPlayLockstepOutboundPacket pendingOutbound[NETPLAY_LOCKSTEP_OUTBOUND_QUEUE_SIZE];
	uint8_t pendingOutboundRead;
	uint8_t pendingOutboundWrite;
	uint8_t pendingOutboundCount;
};

typedef struct GBASIONetPlayLockstepDriver NetPlayLockstepDriver;

void GBASIONetPlayLockstepDriverCreate(struct GBASIONetPlayLockstepDriver*, struct mLockstepUser*);
void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver*);

bool GBASIONetPlayLockstepDriverConnect(struct GBASIONetPlayLockstepDriver*, const char* host, uint16_t port);
bool GBASIONetPlayLockstepDriverConnectDefault(struct GBASIONetPlayLockstepDriver*);
bool GBASIONetPlayLockstepDriverIsConnected(const struct GBASIONetPlayLockstepDriver*);

CXX_GUARD_END

#endif
