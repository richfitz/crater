# dde

[![Project Status: WIP - Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](http://www.repostatus.org/badges/latest/wip.svg)](http://www.repostatus.org/#wip)
[![Travis-CI Build Status](https://travis-ci.org/richfitz/crater.svg?branch=master)](https://travis-ci.org/richfitz/crater)
[![codecov.io](https://codecov.io/github/richfitz/crater/coverage.svg?branch=master)](https://codecov.io/github/richfitz/crater?branch=master)

R client to the [crate](https://crate.io) database.

```
mkdir -p tests/testthat/shared
docker run -d -p 4200:4200 -p 4300:4300 --name crater_testing -v ${PWD}/tests/testthat/shared:/host crate
```

```
docker run --rm -it --link crater_testing:crate crate crash --hosts crate:4200
```
