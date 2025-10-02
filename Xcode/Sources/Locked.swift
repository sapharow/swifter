//
//  Locked.swift
//  Swifter
//
//  Created by Iskandar Safarov on 2/10/2025.
//  Copyright © 2025 Damian Kołakowski. All rights reserved.
//

import Dispatch

@propertyWrapper
final class Locked<Value> {
    private var value: Value

    private var lock = os_unfair_lock_s()
    private var mtx = pthread_mutex_t()

    init(wrappedValue: Value) {
        self.value = wrappedValue
        if #available(iOS 10.0, *) {
            lock = os_unfair_lock_s()
        } else {
            mtx = pthread_mutex_t()
            var attr = pthread_mutexattr_t()
            pthread_mutexattr_init(&attr)
            pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL)
            pthread_mutex_init(&mtx, &attr)
            pthread_mutexattr_destroy(&attr)
        }
    }

    var wrappedValue: Value {
        get { with { $0 } }
        set { with { $0 = newValue } }
    }

    // Atomic read-modify-write
    var projectedValue: Locked<Value> { self }

    @inline(__always)
    private func with<R>(_ body: (inout Value) throws -> R) rethrows -> R {
        if #available(iOS 10.0, *) {
            os_unfair_lock_lock(&lock)
            defer { os_unfair_lock_unlock(&lock) }
            return try body(&value)
        } else {
            pthread_mutex_lock(&mtx)
            defer { pthread_mutex_unlock(&mtx) }
            return try body(&value)
        }
    }
}

