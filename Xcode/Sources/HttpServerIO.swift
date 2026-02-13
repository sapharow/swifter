//
//  HttpServer.swift
//  Swifter
//
//  Copyright (c) 2014-2016 Damian Kołakowski. All rights reserved.
//

import Foundation

public protocol HttpServerIODelegate: AnyObject {
    func socketConnectionReceived(_ socket: Socket)
}

open class HttpServerIO {

    public weak var delegate: HttpServerIODelegate?

    private var socket = Socket(socketFileDescriptor: -1)
    private var sockets = Set<Socket>()

    public enum HttpServerIOState: Int32 {
        case starting
        case running
        case stopping
        case stopped
    }

    @Locked<HttpServerIOState> public var state = .stopped
    private let lock = NSLock()

    public var operating: Bool { return self.state == .running }

    /// String representation of the IPv4 address to receive requests from.
    /// It's only used when the server is started with `forceIPv4` option set to true.
    /// Otherwise, `listenAddressIPv6` will be used.
    public var listenAddressIPv4: String?

    /// String representation of the IPv6 address to receive requests from.
    /// It's only used when the server is started with `forceIPv4` option set to false.
    /// Otherwise, `listenAddressIPv4` will be used.
    public var listenAddressIPv6: String?

    private let queue = DispatchQueue(label: "swifter.httpserverio.clientsockets")
    private let connectionGroup = DispatchGroup()

    public func port() throws -> Int {
        lock.lock()
        defer { lock.unlock() }
        return Int(try socket.port())
    }

    public func isIPv4() throws -> Bool {
        return try socket.isIPv4()
    }

    deinit {
        let group = DispatchGroup()
        group.enter()
        stop {
            group.leave()
        }
        group.wait()
    }

    @available(macOS 10.10, *)
    public func start(
        _ port: in_port_t = 8080,
        _ interface: UInt32 = 0,
        forceIPv4: Bool = false,
        priority: DispatchQoS.QoSClass = DispatchQoS.QoSClass.background,
        startedHandler: ((Result<Void, Error>) -> Void)? = nil
    ) {
        stop { [self] in
            lock.lock()
            defer { lock.unlock() }

            guard !operating else {
                startedHandler?(.success(()))
                return
            }

            state = .starting

            let address = forceIPv4 ? listenAddressIPv4 : listenAddressIPv6
            do {
                self.socket = try Socket.tcpSocketForListen(port, forceIPv4, SOMAXCONN, address, interface)
            } catch {
                startedHandler?(.failure(error))
                return
            }

            state = .running

            DispatchQueue.global(qos: priority).async { [weak self] in
                startedHandler?(.success(()))
                while let socket = try? self?.socket.acceptClientSocket() {
                    if let group = self?.connectionGroup {
                        group.enter()
                        DispatchQueue.global(qos: priority).async { [weak self] in
                            defer { group.leave() }
                            guard let strongSelf = self else { return }
                            guard strongSelf.operating else {
                                socket.close()
                                return
                            }

                            strongSelf.queue.discardableSync {
                                strongSelf.sockets.insert(socket)
                            }

                            strongSelf.handleConnection(socket)

                            strongSelf.queue.discardableSync {
                                strongSelf.sockets.remove(socket)
                            }
                        }
                    } else {
                        socket.close()
                    }
                }
                self?.privateStop(completion: nil)
            }
        }
    }

    public func stop(completion: (() -> Void)?) {
        DispatchQueue.global(qos: .default).async { [self] in
            privateStop(completion: completion)
        }
    }

    func privateStop(completion: (() -> Void)?) {
        lock.lock()

        guard operating else {
            lock.unlock()
            completion?()
            return
        }

        state = .stopping

        // Shut down read/write so any thread blocked in read() unblocks; then wait for
        // connection handlers to finish and close their sockets (avoids close-vs-read race).
        queue.sync {
            sockets.forEach { $0.shutdownStream() }
        }

        connectionGroup.wait()

        queue.sync {
            self.sockets.removeAll(keepingCapacity: true)
        }

        socket.close()
        state = .stopped

        lock.unlock()
        completion?()
    }

    open func dispatch(_ request: HttpRequest) -> ([String: String], (HttpRequest) -> HttpResponse) {
        return ([:], { _ in HttpResponse.notFound(nil) })
    }

    private func handleConnection(_ socket: Socket) {
        let parser = HttpParser()
        while self.operating, let request = try? parser.readHttpRequest(socket) {
            let request = request
            request.address = try? socket.peername()
            let (params, handler) = self.dispatch(request)
            request.params = params
            let response = handler(request)
            var keepConnection = parser.supportsKeepAlive(request.headers)
            do {
                if self.operating {
                    keepConnection = try self.respond(socket, response: response, keepAlive: keepConnection)
                }
            } catch {
                print("Failed to send response: \(error)")
            }
            if let session = response.socketSession() {
                delegate?.socketConnectionReceived(socket)
                session(socket)
                break
            }
            if !keepConnection { break }
        }
        socket.close()
    }

    private struct InnerWriteContext: HttpResponseBodyWriter {

        let socket: Socket

        func write(_ file: String.File) throws {
            try socket.writeFile(file)
        }

        func write(_ data: [UInt8]) throws {
            try write(ArraySlice(data))
        }

        func write(_ data: ArraySlice<UInt8>) throws {
            try socket.writeUInt8(data)
        }

        func write(_ data: NSData) throws {
            try socket.writeData(data)
        }

        func write(_ data: Data) throws {
            try socket.writeData(data)
        }
    }

    private func respond(_ socket: Socket, response: HttpResponse, keepAlive: Bool) throws -> Bool {
        guard self.operating else { return false }

        // Some web-socket clients (like Jetfire) expects to have header section in a single packet.
        // We can't promise that but make sure we invoke "write" only once for response header section.

        var responseHeader = String()

        responseHeader.append("HTTP/1.1 \(response.statusCode) \(response.reasonPhrase)\r\n")

        let content = response.content()

        if content.length >= 0 {
            responseHeader.append("Content-Length: \(content.length)\r\n")
        }

        if keepAlive && content.length != -1 {
            responseHeader.append("Connection: keep-alive\r\n")
        }

        for (name, value) in response.headers() {
            responseHeader.append("\(name): \(value)\r\n")
        }

        responseHeader.append("\r\n")

        try socket.writeUTF8(responseHeader)

        if let writeClosure = content.write {
            let context = InnerWriteContext(socket: socket)
            try writeClosure(context)
        }

        return keepAlive && content.length != -1
    }
}

extension DispatchQueue {

    @discardableResult
    func discardableSync<T>(_ closure: () throws -> T) rethrows -> T {
        try sync(execute: closure)
    }

}
