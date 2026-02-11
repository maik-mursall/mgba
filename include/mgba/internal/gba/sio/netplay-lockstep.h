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

struct GBASIONetPlayLockstepTransferResult {
	uint32_t sequence;
	enum GBASIOMode mode;
	int attached;
	uint16_t multiData[MAX_GBAS];
	uint32_t normalData[MAX_GBAS];
};

struct GBASIONetPlayLockstepDriver {
	struct GBASIODriver d;
	struct mTimingEvent event;
	struct mLockstepUser* user;

	Socket socket;
#ifndef DISABLE_THREADING
	Thread thread;
	Mutex mutex;
	bool threadRunning;
#endif

	bool connected;
	bool stopping;
	bool asleep;
	bool waitingForTransfer;
	bool transferActive;
	uint32_t transferSequence;

	int playerId;
	int attached;
	bool present[MAX_GBAS];
	enum GBASIOMode otherModes[MAX_GBAS];
	enum GBASIOMode mode;

	bool stateDirty;
	bool playerIdChanged;
	bool pendingDisconnect;

	bool pendingBegin;
	uint32_t pendingBeginSequence;
	enum GBASIOMode pendingBeginMode;

	bool pendingResult;
	struct GBASIONetPlayLockstepTransferResult pendingTransferResult;
};

typedef struct GBASIONetPlayLockstepDriver NetPlayLockstepDriver;

void GBASIONetPlayLockstepDriverCreate(struct GBASIONetPlayLockstepDriver*, struct mLockstepUser*);
void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver*);

bool GBASIONetPlayLockstepDriverConnect(struct GBASIONetPlayLockstepDriver*, const char* host, uint16_t port);
bool GBASIONetPlayLockstepDriverConnectDefault(struct GBASIONetPlayLockstepDriver*);
bool GBASIONetPlayLockstepDriverIsConnected(const struct GBASIONetPlayLockstepDriver*);

CXX_GUARD_END

#endif
