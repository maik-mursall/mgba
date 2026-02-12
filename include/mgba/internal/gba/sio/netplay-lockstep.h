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

struct GBASIONetPlayLockstepTransferResult {
	uint32_t sequence;
	enum GBASIOMode mode;
	int attached;
	uint16_t multiData[MAX_GBAS];
	uint32_t normalData[MAX_GBAS];
};

struct GBASIONetPlayLockstepPendingBegin {
	uint32_t sequence;
	enum GBASIOMode mode;
	uint8_t attached;
	uint16_t siocnt;
	uint32_t siocntWriteGeneration;
	uint32_t siomltWriteGeneration;
	uint8_t deferEvents;
	uint8_t deferSendEvents;
};

struct GBASIONetPlayLockstepDriver {
	struct GBASIODriver d;
	struct mTimingEvent event;
	struct mLockstepUser* user;

	Socket socket;
#ifndef DISABLE_THREADING
	Thread thread;
	Mutex mutex;
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
	uint32_t transferSequence;
	uint32_t siocntWriteGeneration;
	uint32_t siomltWriteGeneration;

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
};

typedef struct GBASIONetPlayLockstepDriver NetPlayLockstepDriver;

void GBASIONetPlayLockstepDriverCreate(struct GBASIONetPlayLockstepDriver*, struct mLockstepUser*);
void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver*);

bool GBASIONetPlayLockstepDriverConnect(struct GBASIONetPlayLockstepDriver*, const char* host, uint16_t port);
bool GBASIONetPlayLockstepDriverConnectDefault(struct GBASIONetPlayLockstepDriver*);
bool GBASIONetPlayLockstepDriverIsConnected(const struct GBASIONetPlayLockstepDriver*);

CXX_GUARD_END

#endif
