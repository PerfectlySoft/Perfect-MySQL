# Perfect - MySQL Connector

[![GitHub version](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-MySQL.svg)](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-MySQL)

This project provides a Swift wrapper around the MySQL client library, enabling access to MySQL database servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift 3.0 tool chain.

## OS X Build Notes

This package requires the [Home Brew](http://brew.sh) build of MySQL.

To install Home Brew:

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

To install MySQL:

```
brew install mysql
```

## Linux Build Notes

Ensure that you have installed mysql-client.

```
sudo apt-get install mysql-client
```

## Building

Add this project as a dependency in your Package.swift file.

```
.Package(url:"https://github.com/PerfectlySoft/Perfect-MySQL.git", majorVersion: 0, minor: 1)
```