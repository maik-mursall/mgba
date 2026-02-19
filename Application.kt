import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

private const val DEFAULT_HOST = "0.0.0.0"
private const val DEFAULT_PORT = 7777
private const val MAX_PLAYERS = 4
private const val MAX_PAYLOAD_SIZE = 4096
private const val SEND_TIMEOUT_MS = 2000L
private var isInfo = true
private var isDebug = true

private fun logInfo(message: String) {
    if (isInfo) {
        println(message)
    }
}

private fun logDebug(message: String) {
    if (isDebug) {
        println(message)
    }
}

private const val MSG_HELLO = 0x01
private const val MSG_MODE = 0x02
private const val MSG_TRANSFER_START = 0x03
private const val MSG_TRANSFER_DATA = 0x04

private const val MSG_STATE = 0x10
private const val MSG_TRANSFER_BEGIN = 0x11
private const val MSG_TRANSFER_RESULT = 0x12

private data class TransferSample(
    val send16: Int,
    val send32: Int
)

private data class TransferContext(
    val sequence: Int,
    val mode: Int,
    val expected: MutableSet<Int>,
    val samples: MutableMap<Int, TransferSample> = mutableMapOf()
)

private class ClientSession(
    var id: Int,
    val socket: Socket,
    val input: ByteReadChannel,
    val output: ByteWriteChannel
) {
    val writeLock = Mutex()
    var mode: Int = 0xFF

    suspend fun send(type: Int, payload: ByteArray) {
        val header = ByteArray(8)
        header[0] = type.toByte()
        putIntBE(header, 4, payload.size)
        writeLock.withLock {
            output.writeFully(header, 0, header.size)
            if (payload.isNotEmpty()) {
                output.writeFully(payload, 0, payload.size)
            }
            output.flush()
        }
    }
}

private class RelayCoordinator {
    private val stateLock = Mutex()
    private val transferLock = Mutex()
    private val clients = mutableMapOf<Int, ClientSession>()
    private var transfer: TransferContext? = null
    private var nextSequence = 1

    suspend fun attach(socket: Socket): ClientSession? {
        val session = ClientSession(
            id = -1,
            socket = socket,
            input = socket.openReadChannel(),
            output = socket.openWriteChannel(autoFlush = false)
        )
        stateLock.withLock {
            val id = firstFreeId() ?: return null
            session.id = id
            clients[id] = session
            logInfo("Client connected as player $id")
        }
        broadcastState()
        return session
    }

    suspend fun detach(session: ClientSession) {
        var finalize = false
        transferLock.withLock {
            stateLock.withLock {
                clients.remove(session.id)
                transfer?.let { ctx ->
                    ctx.expected.remove(session.id)
                    ctx.samples.remove(session.id)
                    if (ctx.expected.all { ctx.samples.containsKey(it) }) {
                        finalize = true
                    }
                }
                logInfo("Client disconnected from player ${session.id}")
            }
            if (finalize) {
                finalizeTransferLocked()
            }
        }
        broadcastState()
        runCatching { session.socket.close() }
    }

    suspend fun handleFrame(session: ClientSession, type: Int, payload: ByteArray) {
        when (type) {
            MSG_HELLO -> {
                if (payload.isNotEmpty()) {
                    val requested = payload[0].toInt() and 0xFF
                    if (requested < MAX_PLAYERS) {
                        maybeReassign(session, requested)
                    }
                }
                broadcastState()
            }

            MSG_MODE -> {
                if (payload.isNotEmpty()) {
                    session.mode = payload[0].toInt() and 0xFF
                    broadcastState()
                }
            }

            MSG_TRANSFER_START -> {
                logDebug("Received TRANSFER_START from player ${session.id} (${payload.size} bytes)")
                transferLock.withLock {
                    handleTransferStartLocked(session, payload)
                }
            }

            MSG_TRANSFER_DATA -> {
                logDebug("Received TRANSFER_DATA from player ${session.id} (${payload.size} bytes)")
                transferLock.withLock {
                    handleTransferDataLocked(session, payload)
                }
            }
        }
    }

    private suspend fun maybeReassign(session: ClientSession, requested: Int) {
        var changed = false
        stateLock.withLock {
            if (requested != session.id && !clients.containsKey(requested)) {
                clients.remove(session.id)
                session.id = requested
                clients[requested] = session
                changed = true
                logInfo("Client reassigned to player $requested")
            }
        }
        if (changed) {
            broadcastState()
        }
    }

    private suspend fun handleTransferStartLocked(session: ClientSession, payload: ByteArray) {
        if (payload.size < 16) {
            return
        }
        val requestedSequence = readIntBE(payload, 0)
        val mode = payload[4].toInt() and 0xFF
        val siocnt = readU16BE(payload, 6)
        val send16 = readU16BE(payload, 8)
        val send32 = readIntBE(payload, 12)
        val hasStartCycle = payload.size >= 20
        val startCycle = if (hasStartCycle) readIntBE(payload, 16) else 0
        val sample = TransferSample(send16, send32)

        var beginTargets = emptyList<ClientSession>()
        var beginPayload = byteArrayOf()
        var finalizeNow = false

        stateLock.withLock {
            if (session.id != 0) {
                return
            }
            if (transfer != null) {
                return
            }
            val expectedIds = clients.keys.toMutableSet()
            if (expectedIds.isEmpty()) {
                return
            }
            val useSequence = nextTransferSequenceLocked()
            val ctx = TransferContext(useSequence, mode, expectedIds)
            ctx.samples[session.id] = sample
            transfer = ctx
            beginPayload = if (hasStartCycle) ByteArray(16) else ByteArray(12)
            putIntBE(beginPayload, 0, ctx.sequence)
            beginPayload[4] = mode.toByte()
            beginPayload[5] = session.id.toByte()
            beginPayload[6] = expectedIds.size.toByte()
            beginPayload[7] = 0
            putU16BE(beginPayload, 8, siocnt)
            beginPayload[10] = 0
            beginPayload[11] = 0
            if (hasStartCycle) {
                putIntBE(beginPayload, 12, startCycle)
            }
            beginTargets = expectedIds
                .filter { it != session.id }
                .filter { !ctx.samples.containsKey(it) }
                .mapNotNull { clients[it] }
            finalizeNow = ctx.expected.all { ctx.samples.containsKey(it) }
            if (requestedSequence != 0 && requestedSequence != ctx.sequence) {
                logDebug("Transfer start from player ${session.id} requested sequence $requestedSequence; relay assigned ${ctx.sequence}")
            }
            if (hasStartCycle) {
                logDebug("Transfer ${ctx.sequence} started in mode $mode at cycle $startCycle; expected=${ctx.expected.sorted()} beginTargets=${beginTargets.map { it.id }}")
            } else {
                logDebug("Transfer ${ctx.sequence} started in mode $mode; expected=${ctx.expected.sorted()} beginTargets=${beginTargets.map { it.id }}")
            }
        }

        for (target in beginTargets) {
            logDebug("Transfer ${readIntBE(beginPayload, 0)}: forwarding BEGIN to player ${target.id}")
            val sent = safeSend(target, MSG_TRANSFER_BEGIN, beginPayload)
            logDebug("Transfer ${readIntBE(beginPayload, 0)}: BEGIN to player ${target.id} ${if (sent) "sent" else "failed"}")
        }
        if (finalizeNow) {
            finalizeTransferLocked()
        }
    }

    private suspend fun handleTransferDataLocked(session: ClientSession, payload: ByteArray) {
        if (payload.size < 16) {
            return
        }
        val sequence = readIntBE(payload, 0)
        val mode = payload[4].toInt() and 0xFF
        val send16 = readU16BE(payload, 8)
        val send32 = readIntBE(payload, 12)
        val sample = TransferSample(send16, send32)
        var finalizeNow = false
        var accepted = false
        var rejectedReason: String? = null
        var effectiveSequence = sequence
        stateLock.withLock {
            val ctx = transfer
            if (ctx == null) {
                rejectedReason = "no active transfer"
                return@withLock
            }
            if (sequence == 0) {
                effectiveSequence = ctx.sequence
                rejectedReason = "invalid sequence 0"
                return@withLock
            }
            effectiveSequence = sequence
            if (ctx.sequence != effectiveSequence) {
                rejectedReason = "sequence mismatch (expected ${ctx.sequence}, got $sequence)"
                return@withLock
            }
            if (ctx.mode != mode) {
                rejectedReason = "mode mismatch (expected ${ctx.mode}, got $mode)"
                return@withLock
            }
            if (!ctx.expected.contains(session.id)) {
                rejectedReason = "unexpected player ${session.id}"
                return@withLock
            }
            accepted = ctx.samples.putIfAbsent(session.id, sample) == null
            finalizeNow = ctx.expected.all { ctx.samples.containsKey(it) }
        }
        if (accepted) {
            logDebug("Transfer $effectiveSequence: received data from player ${session.id}")
        } else if (rejectedReason != null) {
            logDebug("Transfer $effectiveSequence: ignored data from player ${session.id}: $rejectedReason")
        }
        if (finalizeNow) {
            finalizeTransferLocked()
        }
    }

    private suspend fun finalizeTransferLocked() {
        var payload = byteArrayOf()
        var targets = emptyList<ClientSession>()
        stateLock.withLock {
            val ctx = transfer ?: return
            if (!ctx.expected.all { ctx.samples.containsKey(it) }) {
                return
            }
            val multiData = IntArray(MAX_PLAYERS) { 0xFFFF }
            val normalData = IntArray(MAX_PLAYERS) { 0xFFFFFFFF.toInt() }
            for ((id, sample) in ctx.samples) {
                if (id in 0 until MAX_PLAYERS) {
                    multiData[id] = sample.send16 and 0xFFFF
                    if (id > 0) {
                        normalData[id - 1] = if (ctx.mode == 0) {
                            sample.send32 and 0xFF
                        } else {
                            sample.send32
                        }
                    }
                }
            }
            payload = ByteArray(32)
            putIntBE(payload, 0, ctx.sequence)
            payload[4] = ctx.mode.toByte()
            payload[5] = ctx.expected.size.toByte()
            payload[6] = 0
            payload[7] = 0
            for (i in 0 until MAX_PLAYERS) {
                putU16BE(payload, 8 + i * 2, multiData[i])
            }
            for (i in 0 until MAX_PLAYERS) {
                putIntBE(payload, 16 + i * 4, normalData[i])
            }
            targets = ctx.expected.mapNotNull { clients[it] }
            transfer = null
            logDebug("Transfer ${ctx.sequence} finalized")
        }
        for (target in targets) {
            logDebug("Transfer ${readIntBE(payload, 0)}: forwarding RESULT to player ${target.id}")
            val sent = safeSend(target, MSG_TRANSFER_RESULT, payload)
            logDebug("Transfer ${readIntBE(payload, 0)}: RESULT to player ${target.id} ${if (sent) "sent" else "failed"}")
        }
    }

    private fun nextTransferSequenceLocked(): Int {
        var sequence = nextSequence++
        if (sequence == 0) {
            sequence = nextSequence++
        }
        return sequence
    }

    private suspend fun broadcastState() {
        var targets = emptyList<Pair<ClientSession, ByteArray>>()
        stateLock.withLock {
            val presentMask = clients.keys.fold(0) { acc, id -> acc or (1 shl id) }
            val modes = IntArray(MAX_PLAYERS) { 0xFF }
            for ((id, client) in clients) {
                if (id in 0 until MAX_PLAYERS) {
                    modes[id] = client.mode
                }
            }
            targets = clients.values.map { client ->
                val payload = ByteArray(8)
                payload[0] = client.id.toByte()
                payload[1] = clients.size.toByte()
                payload[2] = presentMask.toByte()
                payload[3] = 0
                for (i in 0 until MAX_PLAYERS) {
                    payload[4 + i] = modes[i].toByte()
                }
                client to payload
            }
        }
        for ((client, payload) in targets) {
            safeSend(client, MSG_STATE, payload)
        }
    }

    private suspend fun safeSend(client: ClientSession, type: Int, payload: ByteArray): Boolean {
        val ok = runCatching {
            withTimeout(SEND_TIMEOUT_MS) {
                client.send(type, payload)
            }
        }.isSuccess
        if (!ok) {
            logInfo("Send failed: type=$type to player ${client.id}, closing socket")
            runCatching { client.socket.close() }
            return false
        }
        return true
    }

    private fun firstFreeId(): Int? {
        for (id in 0 until MAX_PLAYERS) {
            if (!clients.containsKey(id)) {
                return id
            }
        }
        return null
    }
}

private suspend fun readFrame(input: ByteReadChannel): Pair<Int, ByteArray>? {
    val header = ByteArray(8)
    if (!readFullyOrNull(input, header)) {
        return null
    }
    val type = header[0].toInt() and 0xFF
    val size = readIntBE(header, 4)
    if (size !in 0..MAX_PAYLOAD_SIZE) {
        return null
    }
    val payload = ByteArray(size)
    if (size > 0 && !readFullyOrNull(input, payload)) {
        return null
    }
    return type to payload
}

private suspend fun readFullyOrNull(channel: ByteReadChannel, data: ByteArray): Boolean {
    return try {
        channel.readFully(data, 0, data.size)
        true
    } catch (_: Throwable) {
        false
    }
}

private fun putU16BE(dst: ByteArray, offset: Int, value: Int) {
    dst[offset] = ((value ushr 8) and 0xFF).toByte()
    dst[offset + 1] = (value and 0xFF).toByte()
}

private fun readU16BE(src: ByteArray, offset: Int): Int {
    return ((src[offset].toInt() and 0xFF) shl 8) or (src[offset + 1].toInt() and 0xFF)
}

private fun putIntBE(dst: ByteArray, offset: Int, value: Int) {
    dst[offset] = ((value ushr 24) and 0xFF).toByte()
    dst[offset + 1] = ((value ushr 16) and 0xFF).toByte()
    dst[offset + 2] = ((value ushr 8) and 0xFF).toByte()
    dst[offset + 3] = (value and 0xFF).toByte()
}

private fun readIntBE(src: ByteArray, offset: Int): Int {
    return ((src[offset].toInt() and 0xFF) shl 24) or
            ((src[offset + 1].toInt() and 0xFF) shl 16) or
            ((src[offset + 2].toInt() and 0xFF) shl 8) or
            (src[offset + 3].toInt() and 0xFF)
}

fun main() = runBlocking {
    val selector = ActorSelectorManager(Dispatchers.IO)
    val server: ServerSocket = aSocket(selector).tcp().bind(DEFAULT_HOST, DEFAULT_PORT)
    val relay = RelayCoordinator()
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    logInfo("NetPlay relay listening on $DEFAULT_HOST:$DEFAULT_PORT")
    try {
        while (true) {
            val socket = server.accept()
            scope.launch {
                val session = relay.attach(socket)
                if (session == null) {
                    runCatching { socket.close() }
                    return@launch
                }
                try {
                    while (true) {
                        val frame = readFrame(session.input) ?: break
                        relay.handleFrame(session, frame.first, frame.second)
                    }
                } finally {
                    relay.detach(session)
                }
            }
        }
    } finally {
        scope.coroutineContext.cancel()
        server.close()
        selector.close()
    }
}
