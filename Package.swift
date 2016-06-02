//
//  Package.swift
//  Perfect-MySQL
//
//  Created by Kyle Jessup on 3/22/16.
//	Copyright (C) 2016 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PackageDescription

#if os(OSX)
let package = Package(
    name: "MySQL",
    targets: [],
    dependencies: [
                      .Package(url: "https://github.com/PerfectlySoft/Perfect-mysqlclient.git", versions: Version(0,0,0)..<Version(10,0,0))
    ],
    exclude: ["Sources/mysqlclient"]
)
#else
let package = Package(
    name: "MySQL",
    targets: [],
    dependencies: [
                      .Package(url: "https://github.com/PerfectlySoft/Perfect-mysqlclient-Linux.git", versions: Version(0,0,0)..<Version(10,0,0))
    ],
    exclude: ["Sources/mysqlclient"]
)
#endif

products.append(Product(name: "MySQL", type: .Library(.Dynamic), modules: "MySQL"))
