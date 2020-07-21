<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-0
-->
# Pravega-Storage-Adapters [![Build Status](https://travis-ci.org/pravega/pravega-storage-adapters.svg?branch=master)](https://travis-ci.org/pravega/pravega-storage-adapters/builds) [![codecov](https://codecov.io/gh/pravega/pravega-storage-adapters/branch/master/graph/badge.svg?token=6xOvaR0sIa)](https://codecov.io/gh/pravega/pravega-storage-adapters) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Version](https://img.shields.io/github/release/pravega/pravega.svg)](https://github.com/pravega/pravega-storage-adapters/releases)

Pravega-Storage-Adapters is an open source tier 2 provider to accompany Pravega.

To learn more about Pravega, visit http://pravega.io

## Prerequisites
# Pravega [![Build Status](https://travis-ci.org/pravega/pravega.svg?branch=master)](https://travis-ci.org/pravega/pravega/builds) [![codecov](https://codecov.io/gh/pravega/pravega/branch/master/graph/badge.svg?token=6xOvaR0sIa)](https://codecov.io/gh/pravega/pravega) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Version](https://img.shields.io/github/release/pravega/pravega.svg)](https://github.com/pravega/pravega/releases)

## Building Pravega-Storage-Adapters

Checkout the source code:

```
git clone https://github.com/pravega/pravega-storage-adapters.git
cd pravega-storage-adapters
```

Build the pravega-storage-adapters distribution:

```
./gradlew distribution
```

Install pravega-storage-adapters jar files into the local maven repository. This is handy for running the `pravega-samples` locally against a custom version of pravega.

```
./gradlew install
```

Running unit tests:

```
./gradlew test
```


## Releases

The latest pravega-storage-adapters releases can be found on the [Github Release](https://github.com/pravega/pravega-storage-adapters/releases) project page.

## Running Pravega-Storage-Adapters
Please follow the same instructions as in Pravega repository.

## Support
Please follow the same instructions as in Pravega repository.

## Documentation

The Pravega documentation of is hosted on the website:
<http://pravega.io/docs/latest> or in the [documentation](documentation/src/docs) directory of the source code.

## Contributing

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](documentation/src/docs/contributing.md) we describe how to contribute to Pravega!
You can see the roadmap document [here](documentation/src/docs/roadmap.md).

## About

Pravega-Storage-Adapters is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
