package dev.mursall

import io.ktor.network.selector.ActorSelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.application.log
import io.ktor.utils.io.readUTF8Line
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import java.util.LinkedHashMap
import java.util.concurrent.atomic.AtomicLong

/*
TCP line protocol (ASCII, one command per line).

Mode mapping (matches lockstep.c _modeEnumToInt):
  0 invalid/-1
  1 MULTI
  2 NORMAL_8
  3 NORMAL_32
  4 GPIO
  5 UART
  6 JOYBUS

Event mapping (matches enum GBASIOLockstepEventType):
  0 ATTACH
  1 DETACH
  2 HARD_SYNC
  3 MODE_SET
  4 TRANSFER_START

Client -> server:
  HELLO <mode> <requestedId> <timestamp>
  SET_MODE <mode> <timestamp>
  START_TRANSFER <timestamp> <finishCycle> <txData>
  SUBMIT_DATA <txData>
  ACK
  HARD_SYNC <timestamp>
  TICK <timestamp>
  FINISH <mode> <timestamp>
  DETACH <timestamp>
  PING

Server -> client:
  WELCOME <lockstepId> <playerId> <nAttached> <transferMode> <cycle> <nextHardSync>
  PLAYER_ID <newPlayerId>
  ATTACHED <nAttached>
  EVENT <type> <timestamp> <playerId> <value>
  WAKE
  SLEEP
  TRANSFER_DONE <transferMode> <m0> <m1> <m2> <m3> <n0> <n1> <n2> <n3>
  FINISH_MULTI <d0> <d1> <d2> <d3>
  FINISH_N8 <d>
  FINISH_N32 <d>
  OK <command>
  ERR <reason>
*/

private const val MAX_GBAS = 4
private const val HARD_SYNC_INTERVAL = 0x80000
private const val TARGET_ALL = (1 shl MAX_GBAS) - 1

private object SioMode {
    const val INVALID = 0
    const val MULTI = 1
    const val NORMAL_8 = 2
    const val NORMAL_32 = 3
}

private object EventType {
    const val ATTACH = 0
    const val DETACH = 1
    const val HARD_SYNC = 2
    const val MODE_SET = 3
    const val TRANSFER_START = 4
}

private data class ClientConnection(
    val sendQueue: Channel<String>,
    val connectionId: Long,
    val remoteAddress: String,
) {
    var player: PlayerState? = null
}

private data class PlayerState(
    val lockstepId: Long,
    val conn: ClientConnection,
    var playerId: Int = -1,
    var mode: Int = SioMode.INVALID,
    val otherModes: IntArray = IntArray(MAX_GBAS) { SioMode.INVALID },
    var asleep: Boolean = false,
    var dataReceived: Boolean = false,
    var lastTimestamp: Int = 0,
)

private data class LockstepEvent(
    val type: Int,
    val timestamp: Int,
    val playerId: Int,
    val value: Int = 0,
)

private data class Outbound(
    val conn: ClientConnection,
    val line: String,
)

private class RemoteLockstepCoordinator(
    private val logger: Logger,
) {
    private val mutex = Mutex()

    private val players = LinkedHashMap<Long, PlayerState>()
    private var nextId = 0L

    private val attachedPlayers = LongArray(MAX_GBAS)
    private var nAttached = 0
    private var waiting = 0

    private var transferActive = false
    private var transferMode = SioMode.INVALID
    private var pendingTransferSubmit = 0

    private var cycle = 0
    private var nextHardSync = HARD_SYNC_INTERVAL

    private val multiData = IntArray(MAX_GBAS) { 0xFFFF }
    private val normalData = IntArray(MAX_GBAS) { -1 }

    suspend fun onCommand(conn: ClientConnection, rawLine: String): List<Outbound> = mutex.withLock {
        val line = rawLine.trim()
        if (line.isEmpty()) {
            return@withLock emptyList()
        }

        val parts = line.split(Regex("\\s+"))
        val cmd = parts[0].uppercase()

        if (cmd != "HELLO" && conn.player == null) {
            return@withLock listOf(Outbound(conn, "ERR not_attached_send_HELLO_first"))
        }

        return@withLock when (cmd) {
            "HELLO" -> handleHello(conn, parts)
            "SET_MODE" -> handleSetMode(conn, parts)
            "START_TRANSFER" -> handleStartTransfer(conn, parts)
            "SUBMIT_DATA" -> handleSubmitData(conn, parts)
            "ACK" -> handleAck(conn)
            "HARD_SYNC" -> handleHardSync(conn, parts)
            "TICK" -> handleTick(conn, parts)
            "FINISH" -> handleFinish(conn, parts)
            "DETACH" -> handleDetach(conn, parts)
            "PING" -> listOf(Outbound(conn, "OK PING"))
            else -> listOf(Outbound(conn, "ERR unknown_command"))
        }
    }

    suspend fun onDisconnect(conn: ClientConnection): List<Outbound> = mutex.withLock {
        val player = conn.player ?: return@withLock emptyList()
        val out = mutableListOf<Outbound>()
        removePlayer(out, player, player.lastTimestamp)
        conn.player = null
        return@withLock out
    }

    private fun handleHello(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (conn.player != null) {
            return listOf(Outbound(conn, "ERR already_attached"))
        }
        if (parts.size < 4) {
            return listOf(Outbound(conn, "ERR usage_HELLO_mode_requestedId_timestamp"))
        }

        val mode = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_mode"))
        val requestedId = parts[2].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_requestedId"))
        val timestamp = parts[3].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))

        val out = mutableListOf<Outbound>()
        val before = snapshotPlayerIds()

        val lockstepId = allocateId()
        val player = PlayerState(
            lockstepId = lockstepId,
            conn = conn,
            mode = mode,
            lastTimestamp = timestamp,
        )

        conn.player = player
        players[lockstepId] = player

        reconfigPlayers()
        emitPlayerIdChanges(out, before)

        logger.info(
            "Client#{} HELLO accepted: remote={}, lockstepId={}, playerId={}, requestedId={}, mode={}, attached={}",
            conn.connectionId,
            conn.remoteAddress,
            lockstepId,
            player.playerId,
            requestedId,
            mode,
            nAttached,
        )

        if (player.playerId != 0 && player.playerId in 0 until MAX_GBAS) {
            enqueueEvent(
                out,
                LockstepEvent(EventType.ATTACH, timestamp, player.playerId),
                TARGET_ALL and target(player.playerId).inv(),
            )
            sendModeSnapshotToNewPlayer(out, player, timestamp)
        }

        if (transferActive) {
            abortTransfer(out, player)
            player.asleep = false
        }

        if (player.playerId == 0 && nAttached > 1) {
            waiting = 0
            player.asleep = false
            wakeSecondaries(out)
        }

        setReady(player, player.playerId, player.mode)
        if (players.size == 1) {
            cycle = timestamp
            nextHardSync = HARD_SYNC_INTERVAL
        } else {
            setReady(player, 0, transferMode)
        }

        send(out, conn, "WELCOME $lockstepId ${player.playerId} $nAttached $transferMode $cycle $nextHardSync")
        broadcastAttached(out)
        send(out, conn, "OK HELLO")
        return out
    }

    private fun handleSetMode(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 3) {
            return listOf(Outbound(conn, "ERR usage_SET_MODE_mode_timestamp"))
        }

        val mode = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_mode"))
        val timestamp = parts[2].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))

        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))
        player.lastTimestamp = timestamp

        val out = mutableListOf<Outbound>()

        if (mode != player.mode) {
            if (transferActive && mode != transferMode) {
                abortTransfer(out, player)
            }

            player.mode = mode
            if (player.playerId == 0) {
                transferMode = mode
                waitOnPlayers(out, player, timestamp)
            }

            setReady(player, player.playerId, mode)
            enqueueEvent(
                out,
                LockstepEvent(EventType.MODE_SET, timestamp, player.playerId, mode),
                TARGET_ALL and target(player.playerId).inv(),
            )
        }

        send(out, conn, "OK SET_MODE")
        return out
    }

    private fun handleStartTransfer(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 4) {
            return listOf(Outbound(conn, "ERR usage_START_TRANSFER_timestamp_finishCycle_txData"))
        }

        val timestamp = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))
        val finishCycle = parts[2].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_finishCycle"))
        val txData = parts[3].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_txData"))

        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))
        player.lastTimestamp = timestamp

        val out = mutableListOf<Outbound>()

        if (transferActive) {
            send(out, conn, "ERR transfer_already_active")
            return out
        }
        if (nAttached < 2) {
            send(out, conn, "ERR no_secondary_players")
            return out
        }
        if (player.playerId != 0) {
            send(out, conn, "ERR only_primary_can_start")
            return out
        }

        logger.info(
            "Transfer start: lockstepId={}, playerId={}, timestamp={}, finishCycle={}, txData={}, mode={}, attached={}",
            player.lockstepId,
            player.playerId,
            timestamp,
            finishCycle,
            txData,
            transferMode,
            nAttached,
        )

        if (waiting != 0) {
            logger.info(
                "Transfer start preempting active wait: lockstepId={}, waitingMask=0x{}",
                player.lockstepId,
                waiting.toString(16),
            )
            waiting = 0
            pendingTransferSubmit = 0
            wakePlayer(out, player)
        }

        resetTransferBuffers()
        setData(0, txData)

        enqueueEvent(
            out,
            LockstepEvent(EventType.TRANSFER_START, timestamp, 0, finishCycle),
            TARGET_ALL and target(0).inv(),
        )

        val waitingBefore = waiting
        waitOnPlayers(out, player, timestamp)
        if (waiting == waitingBefore) {
            return out
        }
        transferActive = true
        pendingTransferSubmit = waiting

        send(out, conn, "OK START_TRANSFER")
        return out
    }

    private fun handleSubmitData(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 2) {
            return listOf(Outbound(conn, "ERR usage_SUBMIT_DATA_txData"))
        }

        val txData = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_txData"))
        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))

        if (player.playerId !in 0 until MAX_GBAS) {
            return listOf(Outbound(conn, "ERR invalid_player_id"))
        }

        logger.info(
            "Transfer submit: lockstepId={}, playerId={}, txData={}, mode={}",
            player.lockstepId,
            player.playerId,
            txData,
            transferMode,
        )

        setData(player.playerId, txData)
        if (transferActive && player.playerId in 1 until MAX_GBAS) {
            pendingTransferSubmit = pendingTransferSubmit and target(player.playerId).inv()
        }
        return emptyList()
    }

    private fun handleAck(conn: ClientConnection): List<Outbound> {
        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))
        val out = mutableListOf<Outbound>()
        ackPlayer(out, player)
        return out
    }

    private fun handleHardSync(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 2) {
            return listOf(Outbound(conn, "ERR usage_HARD_SYNC_timestamp"))
        }

        val timestamp = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))
        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))
        player.lastTimestamp = timestamp

        val out = mutableListOf<Outbound>()
        if (player.playerId != 0) {
            send(out, conn, "ERR only_primary_can_hard_sync")
            return out
        }

        hardSync(out, player, timestamp)
        return out
    }

    private fun handleTick(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 2) {
            return listOf(Outbound(conn, "ERR usage_TICK_timestamp"))
        }

        val timestamp = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))
        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))
        player.lastTimestamp = timestamp

        val out = mutableListOf<Outbound>()
        if (player.playerId == 0 && timestamp - cycle >= 0) {
            advanceCycle(timestamp)

            if (!transferActive) {
                wakeSecondaries(out)
            }

            if (nAttached > 1 && nextHardSync < 0 && waiting == 0) {
                hardSync(out, player, timestamp)
            } else if (nAttached < 2 && nextHardSync < 0) {
                nextHardSync = HARD_SYNC_INTERVAL
            }
        }

        return out
    }

    private fun handleFinish(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 3) {
            return listOf(Outbound(conn, "ERR usage_FINISH_mode_timestamp"))
        }

        val mode = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_mode"))
        val timestamp = parts[2].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))

        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))
        player.lastTimestamp = timestamp

        val out = mutableListOf<Outbound>()

        when (mode) {
            SioMode.MULTI -> {
                val values = if (transferMode == SioMode.MULTI && player.dataReceived) {
                    intArrayOf(
                        multiData[0] and 0xFFFF,
                        multiData[1] and 0xFFFF,
                        multiData[2] and 0xFFFF,
                        multiData[3] and 0xFFFF,
                    )
                } else {
                    intArrayOf(0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF)
                }

                if (transferMode == SioMode.MULTI) {
                    player.dataReceived = false
                    if (player.playerId == 0) {
                        hardSync(out, player, timestamp)
                    }
                }

                send(out, conn, "FINISH_MULTI ${values[0]} ${values[1]} ${values[2]} ${values[3]}")
            }

            SioMode.NORMAL_8 -> {
                var data = 0xFF
                if (transferMode == SioMode.NORMAL_8) {
                    if (player.playerId > 0 && player.dataReceived) {
                        data = normalData[player.playerId - 1] and 0xFF
                    }
                    player.dataReceived = false
                    if (player.playerId == 0) {
                        hardSync(out, player, timestamp)
                    }
                }
                send(out, conn, "FINISH_N8 $data")
            }

            SioMode.NORMAL_32 -> {
                var data = 0xFFFFFFFF.toInt()
                if (transferMode == SioMode.NORMAL_32) {
                    if (player.playerId > 0 && player.dataReceived) {
                        data = normalData[player.playerId - 1]
                    }
                    player.dataReceived = false
                    if (player.playerId == 0) {
                        hardSync(out, player, timestamp)
                    }
                }
                send(out, conn, "FINISH_N32 ${data.toUInt()}")
            }

            else -> send(out, conn, "ERR unsupported_mode_for_FINISH")
        }

        return out
    }

    private fun handleDetach(conn: ClientConnection, parts: List<String>): List<Outbound> {
        if (parts.size < 2) {
            return listOf(Outbound(conn, "ERR usage_DETACH_timestamp"))
        }

        val timestamp = parts[1].toIntOrNull() ?: return listOf(Outbound(conn, "ERR invalid_timestamp"))
        val player = conn.player ?: return listOf(Outbound(conn, "ERR not_attached"))

        val out = mutableListOf<Outbound>()
        removePlayer(out, player, timestamp)
        conn.player = null
        send(out, conn, "OK DETACH")
        return out
    }

    private fun removePlayer(out: MutableList<Outbound>, player: PlayerState, timestamp: Int) {
        logger.info(
            "Client detached: lockstepId={}, playerId={}, timestamp={}",
            player.lockstepId,
            player.playerId,
            timestamp,
        )

        val removedId = player.playerId

        if (removedId in 0 until MAX_GBAS) {
            enqueueEvent(
                out,
                LockstepEvent(EventType.DETACH, timestamp, removedId),
                TARGET_ALL and target(removedId).inv(),
            )
        }

        waiting = 0
        transferActive = false
        pendingTransferSubmit = 0

        val before = snapshotPlayerIds()
        players.remove(player.lockstepId)
        reconfigPlayers()
        emitPlayerIdChanges(out, before)

        if (nAttached > 0) {
            broadcastModeSnapshot(out, timestamp)
            players[attachedPlayers[0]]?.let { wakePlayer(out, it) }
        }

        broadcastAttached(out)
    }

    private fun hardSync(out: MutableList<Outbound>, primary: PlayerState, timestamp: Int) {
        if (nAttached < 2) {
            nextHardSync = HARD_SYNC_INTERVAL
            return
        }
        if (waiting != 0) {
            logger.debug(
                "Hard sync ignored while wait is active: lockstepId={}, waitingMask=0x{}, transferActive={}, pendingSubmitMask=0x{}",
                primary.lockstepId,
                waiting.toString(16),
                transferActive,
                pendingTransferSubmit.toString(16),
            )
            return
        }

        logger.info(
            "Hard sync requested: lockstepId={}, timestamp={}, attached={}, waitingMask=0x{}",
            primary.lockstepId,
            timestamp,
            nAttached,
            waiting.toString(16),
        )

        enqueueEvent(
            out,
            LockstepEvent(EventType.HARD_SYNC, timestamp, 0),
            TARGET_ALL and target(0).inv(),
        )
        waitOnPlayers(out, primary, timestamp)
    }

    private fun abortTransfer(out: MutableList<Outbound>, byPlayer: PlayerState) {
        logger.warn(
            "Transfer aborted: byLockstepId={}, byPlayerId={}, attached={}, waitingMask=0x{}",
            byPlayer.lockstepId,
            byPlayer.playerId,
            nAttached,
            waiting.toString(16),
        )

        transferActive = false
        waiting = 0
        pendingTransferSubmit = 0

        if (byPlayer.playerId != 0) {
            players[attachedPlayers[0]]?.let { wakePlayer(out, it) }
        } else {
            wakeSecondaries(out)
        }
    }

    private fun waitOnPlayers(out: MutableList<Outbound>, primary: PlayerState, timestamp: Int) {
        if (nAttached < 2) {
            return
        }

        if (primary.playerId != 0) {
            send(out, primary.conn, "ERR desync_non_primary_wait")
            return
        }

        if (waiting != 0) {
            send(out, primary.conn, "ERR desync_waiting_not_empty")
            return
        }

        if (primary.asleep) {
            send(out, primary.conn, "ERR desync_primary_asleep")
            return
        }

        advanceCycle(timestamp)
        waiting = ((1 shl nAttached) - 1) and target(primary.playerId).inv()

        logger.info(
            "Primary waiting for ACKs: lockstepId={}, timestamp={}, waitingMask=0x{}",
            primary.lockstepId,
            timestamp,
            waiting.toString(16),
        )

        sleepPlayer(out, primary)
        wakeSecondaries(out)
    }

    private fun ackPlayer(out: MutableList<Outbound>, player: PlayerState) {
        if (player.playerId == 0) {
            return
        }

        val playerMask = target(player.playerId)
        if ((waiting and playerMask) == 0) {
            logger.debug(
                "Ignoring out-of-window ACK: lockstepId={}, playerId={}, waitingMask=0x{}",
                player.lockstepId,
                player.playerId,
                waiting.toString(16),
            )
            return
        }
        if (transferActive && (pendingTransferSubmit and playerMask) != 0) {
            logger.warn(
                "Ignoring ACK before SUBMIT_DATA: lockstepId={}, playerId={}, pendingSubmitMask=0x{}",
                player.lockstepId,
                player.playerId,
                pendingTransferSubmit.toString(16),
            )
            return
        }

        val oldWaiting = waiting
        waiting = waiting and playerMask.inv()

        logger.info(
            "ACK received: lockstepId={}, playerId={}, waitingMask=0x{}->0x{}",
            player.lockstepId,
            player.playerId,
            oldWaiting.toString(16),
            waiting.toString(16),
        )

        if (waiting == 0) {
            if (transferActive) {
                forEachAttached { attached ->
                    attached.dataReceived = true
                }
                sendTransferDone(out)
                transferActive = false
                pendingTransferSubmit = 0
            }

            nextHardSync = HARD_SYNC_INTERVAL
            players[attachedPlayers[0]]?.let { wakePlayer(out, it) }
        }

        sleepPlayer(out, player)
    }

    private fun sendTransferDone(out: MutableList<Outbound>) {
        logger.info(
            "Transfer done: mode={}, multi=[{}, {}, {}, {}], normal=[{}, {}, {}, {}], attached={}",
            transferMode,
            multiData[0] and 0xFFFF,
            multiData[1] and 0xFFFF,
            multiData[2] and 0xFFFF,
            multiData[3] and 0xFFFF,
            normalData[0].toUInt(),
            normalData[1].toUInt(),
            normalData[2].toUInt(),
            normalData[3].toUInt(),
            nAttached,
        )

        val line = buildString {
            append("TRANSFER_DONE ")
            append(transferMode)
            append(' ')
            append(multiData[0] and 0xFFFF)
            append(' ')
            append(multiData[1] and 0xFFFF)
            append(' ')
            append(multiData[2] and 0xFFFF)
            append(' ')
            append(multiData[3] and 0xFFFF)
            append(' ')
            append(normalData[0].toUInt())
            append(' ')
            append(normalData[1].toUInt())
            append(' ')
            append(normalData[2].toUInt())
            append(' ')
            append(normalData[3].toUInt())
        }

        forEachAttached { player ->
            send(out, player.conn, line)
        }
    }

    private fun resetTransferBuffers() {
        for (i in 0 until MAX_GBAS) {
            multiData[i] = 0xFFFF
            normalData[i] = -1
        }
    }

    private fun setData(playerId: Int, data: Int) {
        if (playerId !in 0 until MAX_GBAS) {
            return
        }

        when (transferMode) {
            SioMode.MULTI -> multiData[playerId] = data and 0xFFFF
            SioMode.NORMAL_8 -> normalData[playerId] = data and 0xFF
            SioMode.NORMAL_32 -> normalData[playerId] = data
            else -> {
                // Unsupported lockstep mode for transfer payload.
            }
        }
    }

    private fun setReady(activePlayer: PlayerState, playerId: Int, mode: Int) {
        if (playerId !in 0 until MAX_GBAS) {
            return
        }
        activePlayer.otherModes[playerId] = mode
    }

    private fun enqueueEvent(out: MutableList<Outbound>, event: LockstepEvent, targetMask: Int) {
        for (i in 0 until nAttached) {
            if ((targetMask and target(i)) == 0) {
                continue
            }
            val lockstepId = attachedPlayers[i]
            val player = players[lockstepId] ?: continue
            send(out, player.conn, "EVENT ${event.type} ${event.timestamp} ${event.playerId} ${event.value}")
        }
    }

    private fun advanceCycle(timestamp: Int) {
        val delta = timestamp - cycle
        if (delta < 0) {
            return
        }
        nextHardSync -= delta
        cycle = timestamp
    }

    private fun wakeSecondaries(out: MutableList<Outbound>) {
        for (i in 1 until nAttached) {
            val player = players[attachedPlayers[i]] ?: continue
            wakePlayer(out, player)
        }
    }

    private fun wakePlayer(out: MutableList<Outbound>, player: PlayerState) {
        if (!player.asleep) {
            return
        }
        player.asleep = false
        logger.debug("Wake player: lockstepId={}, playerId={}", player.lockstepId, player.playerId)
        send(out, player.conn, "WAKE")
    }

    private fun sleepPlayer(out: MutableList<Outbound>, player: PlayerState) {
        if (player.asleep) {
            return
        }
        player.asleep = true
        logger.debug("Sleep player: lockstepId={}, playerId={}", player.lockstepId, player.playerId)
        send(out, player.conn, "SLEEP")
    }

    private fun broadcastAttached(out: MutableList<Outbound>) {
        for (player in players.values) {
            send(out, player.conn, "ATTACHED $nAttached")
        }
    }

    private fun sendModeSnapshotToNewPlayer(out: MutableList<Outbound>, newPlayer: PlayerState, timestamp: Int) {
        val targetMask = target(newPlayer.playerId)
        forEachAttached { src ->
            if (src.lockstepId == newPlayer.lockstepId) {
                return@forEachAttached
            }
            enqueueEvent(
                out,
                LockstepEvent(EventType.MODE_SET, timestamp, src.playerId, src.mode),
                targetMask,
            )
        }
    }

    private fun broadcastModeSnapshot(out: MutableList<Outbound>, timestamp: Int) {
        forEachAttached { src ->
            enqueueEvent(
                out,
                LockstepEvent(EventType.MODE_SET, timestamp, src.playerId, src.mode),
                TARGET_ALL and target(src.playerId).inv(),
            )
        }
    }

    private fun emitPlayerIdChanges(out: MutableList<Outbound>, before: Map<Long, Int>) {
        for ((lockstepId, player) in players) {
            val old = before[lockstepId]
            if (old == null || old != player.playerId) {
                send(out, player.conn, "PLAYER_ID ${player.playerId}")
            }
        }
    }

    private fun snapshotPlayerIds(): Map<Long, Int> {
        val out = HashMap<Long, Int>(players.size)
        for ((id, player) in players) {
            out[id] = player.playerId
        }
        return out
    }

    private fun reconfigPlayers() {
        attachedPlayers.fill(0)

        val count = players.size
        if (count == 0) {
            nAttached = 0
            return
        }

        if (count == 1) {
            val onlyEntry = players.entries.first()
            attachedPlayers[0] = onlyEntry.key

            val only = onlyEntry.value
            only.playerId = 0

            cycle = only.lastTimestamp
            nextHardSync = HARD_SYNC_INTERVAL

            if (!transferActive) {
                transferMode = only.mode
            }

            nAttached = 1
            return
        }

        for (player in players.values) {
            player.playerId = -1
        }

        var attached = 0
        for ((lockstepId, player) in players) {
            if (attached >= MAX_GBAS) {
                break
            }
            attachedPlayers[attached] = lockstepId
            player.playerId = attached
            attached += 1
        }

        nAttached = attached
    }

    private fun allocateId(): Long {
        while (true) {
            nextId = if (nextId == UInt.MAX_VALUE.toLong()) 1 else nextId + 1
            if (!players.containsKey(nextId)) {
                return nextId
            }
        }
    }

    private fun forEachAttached(block: (PlayerState) -> Unit) {
        for (i in 0 until nAttached) {
            val player = players[attachedPlayers[i]] ?: continue
            block(player)
        }
    }

    private fun target(playerId: Int): Int {
        return if (playerId in 0 until MAX_GBAS) {
            1 shl playerId
        } else {
            0
        }
    }

    private fun send(out: MutableList<Outbound>, conn: ClientConnection, line: String) {
        out += Outbound(conn, line)
    }
}

suspend fun Application.module() {
    val host = environment.config.propertyOrNull("lockstep.host")?.getString() ?: "0.0.0.0"
    val port = environment.config.propertyOrNull("lockstep.port")?.getString()?.toIntOrNull() ?: 6000

    val selector = ActorSelectorManager(Dispatchers.IO)
    val serverSocket = aSocket(selector).tcp().bind(host, port)
    val coordinator = RemoteLockstepCoordinator(log)
    val nextClientId = AtomicLong(1)

    log.info("LockstepCoordinator listening on {}:{}", host, port)

    val acceptJob = launch(Dispatchers.IO) {
        while (isActive) {
            val socket = try {
                serverSocket.accept()
            } catch (_: CancellationException) {
                break
            } catch (t: Throwable) {
                if (!isActive) {
                    break
                }
                log.error("Accept failed", t)
                continue
            }

            val clientId = nextClientId.getAndIncrement()
            val remoteAddress = socket.remoteAddress.toString()
            log.info("Client#{} connected from {}", clientId, remoteAddress)

            launch(Dispatchers.IO) {
                handleClient(socket, coordinator, log, clientId, remoteAddress)
            }
        }
    }

    environment.monitor.subscribe(ApplicationStopping) {
        acceptJob.cancel()
        runBlocking {
            runCatching { serverSocket.close() }
            selector.close()
        }
    }
}

private suspend fun handleClient(
    socket: Socket,
    coordinator: RemoteLockstepCoordinator,
    logger: Logger,
    clientId: Long,
    remoteAddress: String,
) {
    val reader = socket.openReadChannel()
    val writer = socket.openWriteChannel(autoFlush = true)

    val outbox = Channel<String>(Channel.UNLIMITED)
    val conn = ClientConnection(
        sendQueue = outbox,
        connectionId = clientId,
        remoteAddress = remoteAddress,
    )

    val writerJob = kotlinx.coroutines.CoroutineScope(Dispatchers.IO).launch {
        try {
            for (line in outbox) {
                writer.writeStringUtf8(line)
                writer.writeStringUtf8("\n")
            }
        } catch (t: Throwable) {
            logger.error(
                "Client#{} writer failed for {}: {}",
                clientId,
                remoteAddress,
                t.message ?: t::class.simpleName ?: "unknown_error",
            )
            logger.debug("Client#{} writer exception", clientId, t)
        }
    }

    try {
        outbox.send("OK CONNECTED")

        while (true) {
            val line = reader.readUTF8Line() ?: break
            val actions = coordinator.onCommand(conn, line)
            dispatch(actions, logger)
        }
    } catch (t: Throwable) {
        logger.debug("Client#{} handler terminated", clientId, t)
    } finally {
        val lockstepId = conn.player?.lockstepId
        val actions = coordinator.onDisconnect(conn)
        dispatch(actions, logger)

        outbox.close()
        writerJob.cancelAndJoin()

        runCatching { socket.close() }
        logger.info(
            "Client#{} disconnected from {} (lockstepId={})",
            clientId,
            remoteAddress,
            lockstepId ?: "none",
        )
    }
}

private fun dispatch(actions: List<Outbound>, logger: Logger) {
    for (action in actions) {
        val result = action.conn.sendQueue.trySend(action.line)
        if (result.isFailure) {
            logger.error(
                "Dropping outbound line for Client#{} ({}): queue closed, line={}",
                action.conn.connectionId,
                action.conn.remoteAddress,
                action.line,
            )
        }
    }
}
