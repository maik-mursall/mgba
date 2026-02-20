/* Copyright (c) 2026 Maik Mursall
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

#define GBA_SIO_NETPLAY_LOCKSTEP_RX_BUFFER_SIZE 4096
#define GBA_SIO_NETPLAY_LOCKSTEP_MAX_LINE 512
#define GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE 512

struct GBASIONetPlayLockstepDriver {
	struct GBASIODriver d;
	struct mTimingEvent event;
	struct mLockstepUser* user;

	Mutex mutex;
	Socket socket;

	bool connected;
	bool helloSent;
	bool helloPending;
	bool asleep;
	bool transferActive;
	bool dataReceived;

	int playerId;
	int attached;
	enum GBASIOMode mode;
	enum GBASIOMode transferMode;
	enum GBASIOMode otherModes[MAX_GBAS];

	uint16_t multiData[MAX_GBAS];
	uint32_t normalData[MAX_GBAS];
	int32_t cycleOffset;

	uint8_t rxBuffer[GBA_SIO_NETPLAY_LOCKSTEP_RX_BUFFER_SIZE];
	size_t rxBufferSize;

	Thread ioThread;
	bool ioThreadActive;
	bool ioThreadRunning;
	bool pendingTransferStart;
	int32_t pendingTransferStartTimestamp;
	int32_t pendingTransferFinishCycle;
	uint32_t pendingTransferStartsSeen;
	uint32_t pendingTransferStartsProcessed;
	uint32_t pendingTransferStartsOverwritten;
	uint32_t pendingTransferStartsLate;
	size_t lineQueueRead;
	size_t lineQueueWrite;
	char lineQueue[GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE][GBA_SIO_NETPLAY_LOCKSTEP_MAX_LINE];
	size_t outQueueRead;
	size_t outQueueWrite;
	size_t outQueueHeadOffset;
	char outQueue[GBA_SIO_NETPLAY_LOCKSTEP_LINE_QUEUE_SIZE][GBA_SIO_NETPLAY_LOCKSTEP_MAX_LINE];
};

void GBASIONetPlayLockstepDriverCreate(struct GBASIONetPlayLockstepDriver*, struct mLockstepUser*);
void GBASIONetPlayLockstepDriverDestroy(struct GBASIONetPlayLockstepDriver*);

bool GBASIONetPlayLockstepDriverConnect(struct GBASIONetPlayLockstepDriver*, const char* host, uint16_t port);
bool GBASIONetPlayLockstepDriverConnectDefault(struct GBASIONetPlayLockstepDriver*);
void GBASIONetPlayLockstepDriverDisconnect(struct GBASIONetPlayLockstepDriver*);
bool GBASIONetPlayLockstepDriverIsConnected(const struct GBASIONetPlayLockstepDriver*);

CXX_GUARD_END

#endif
