#!/bin/bash

 ps aux -ww | grep Agent | grep -v grep | awk ' { print $2 } ' | xargs kill
