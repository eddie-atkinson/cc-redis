#!/bin/bash

redis-cli SET foo 1
redis-cli SET bar 2
redis-cli SET baz 3

redis-cli -p 8000 GET foo
redis-cli -p 8000 GET bar
redis-cli -p 8000 GET baz
