# Perfect - MySQL Connector

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>  
    <a href="http://stackoverflow.com/questions/tagged/perfect" target="_blank">
        <img src="http://www.perfect.org/github/perfect_gh_button_2_SO.jpg" alt="Stack Overflow" />
    </a>  
    <a href="https://twitter.com/perfectlysoft" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg" alt="Follow Perfect on Twitter" />
    </a>  
    <a href="http://perfect.ly" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg" alt="Join the Perfect Slack" />
    </a>
</p>

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat" alt="Swift 3.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
    <a href="http://twitter.com/PerfectlySoft" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat" alt="PerfectlySoft Twitter">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>


This project provides a Swift wrapper around the MySQL client library, enabling access to MySQL database servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift 3.0 tool chain.

## Issues

We are transitioning to using JIRA for all bugs and support related issues, therefore the GitHub issues has been disabled.

If you find a mistake, bug, or any other helpful suggestion you'd like to make on the docs please head over to [http://jira.perfect.org:8080/servicedesk/customer/portal/1](http://jira.perfect.org:8080/servicedesk/customer/portal/1) and raise it.

A comprehensive list of open issues can be found at [http://jira.perfect.org:8080/projects/ISS/issues](http://jira.perfect.org:8080/projects/ISS/issues)


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

Unfortunately, at this point in time you will need to edit the mysqlclient.pc file located here:

```
/usr/local/lib/pkgconfig/mysqlclient.pc
```

Remove the occurrance of "-fno-omit-frame-pointer". This file is read-only by default so you will need to change that first.

If you get a **link error** while build in Xcode, please, close XCode, open a New terminal, go to the place where you have your Packages.swift and build the project again:

```
swift package generate-xcodeproj
```
After this you have to set the path for libraries again.



## Linux Build Notes

Ensure that you have installed libmysqlclient-dev for MySQL version *5.6 or greater*.

```
sudo apt-get install libmysqlclient-dev
```

Please note that Ubuntu 14 defaults to including a version of MySQL client which will not compile with this package. Install MySQL client version 5.6 or greater.

## Building

Add this project as a dependency in your Package.swift file.

``` swift
.Package(url:"https://github.com/PerfectlySoft/Perfect-MySQL.git", majorVersion: 2)
```

## QuickStart

The following will clone an empty starter project:
```
git clone https://github.com/PerfectlySoft/PerfectTemplate.git
cd PerfectTemplate
```
Add to the Package.swift file the dependency:
``` swift
let package = Package(
 name: "PerfectTemplate",
 targets: [],
 dependencies: [
     .Package(url:"https://github.com/PerfectlySoft/Perfect-HTTPServer.git", majorVersion: 2),
     .Package(url:"https://github.com/PerfectlySoft/Perfect-MySQL.git", majorVersion: 2)
    ]
)
```
Create the Xcode project:
```
swift package generate-xcodeproj
```
Open the generated PerfectTemplate.xcodeproj file in Xcode.

The project will now build in Xcode and start a server on localhost port 8181.

Important: When a dependancy has been added to the project, the Swift Package Manager must be invoked to generate a new Xcode project file. Be aware that any customizations that have been made to this file will be lost.

## Creating a MySQL Connection and selecting data from a table

Open main.swift from the Sources directory and edit according to the following instructions:

Update import statements to include 
``` swift
import PerfectLib
import PerfectHTTP
import PerfectHTTPServer
```
Create an instance of HTTPServer and add routes:
``` swift
// Create HTTP server.
let server = HTTPServer()

// Register your own routes and handlers
var routes = Routes()
routes.add(method: .get, uri: "/", handler: {
		request, response in
		response.appendBody(string: "<html><title>Hello, world!</title><body>Hello, world!</body></html>")
		response.completed()
	}
)

//This route will be used to fetch data from the mysql database
routes.add(method: .get, uri: "/use", handler: useMysql)

// Add the routes to the server.
server.addRoutes(routes)
```

Verify server settings: 
``` swift
// Set a listen port of 8181
server.serverPort = 8181

// Set a document root.
// This is optional. If you do not want to serve static content then do not set this.
// Setting the document root will automatically add a static file handler for the route /**
server.documentRoot = "./webroot"
```

Gather command line options and further configure the server.
Run the server with --help to see the list of supported arguments.
Command line arguments will supplant any of the values set above.
``` swift
configureServer(server)
```
Launch the server. Remember that any command after server.start() will not be reached.
``` swift
do {
	// Launch the HTTP server.
	try server.start()
    
} catch PerfectError.networkError(let err, let msg) {
	print("Network error thrown: \(err) \(msg)")
}
```

Add a file to your project, making sure that it is stored in the Sources directory of your file structure. Lets name it mysql_quickstart.swift for example.

Import required libraries:
``` swift
import PerfectLib
import MySQL
import PerfectHTTP
```

Setup the credentials for your connection: 
``` swift
let testHost = "127.0.0.1"
let testUser = "test"
// PLEASE change to whatever your actual password is before running these tests
let testPassword = "password"
let testSchema = "schema"
```

Create an instance of the MySQL class
``` swift
let dataMysql = MySQL()
```

This function, referenced from the route in main.swift, will setup and use a MySQL connection

``` swift
public func useMysql(_ request: HTTPRequest, response: HTTPResponse) {
    
    // need to make sure something is available.
    guard dataMysql.connect(host: testHost, user: testUser, password: testPassword ) else {
        Log.info(message: "Failure connecting to data server \(testHost)")
        return
    }

    defer {
        dataMysql.close()  // defer ensures we close our db connection at the end of this request
    }
    
    //set database to be used, this example assumes presence of a users table and run a raw query, return failure message on a error
    guard dataMysql.selectDatabase(named: testSchema) && dataMysql.query(statement: "select * from users limit 1") else {
        Log.info(message: "Failure: \(dataMysql.errorCode()) \(dataMysql.errorMessage())")
        
        return
    }
    
    //store complete result set
    let results = dataMysql.storeResults()
    
    //setup an array to store results
    var resultArray = [[String?]]()
    
    while let row = results?.next() {
        resultArray.append(row)
        
    }
   
   //return array to http response
   response.appendBody(string: "<html><title>Mysql Test</title><body>\(resultArray.debugDescription)</body></html>")
    response.completed()
    
}
```

Additionally, there are more complex Statement constructors, and potential object designs which can further abstract the process of interacting with your data.



## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
