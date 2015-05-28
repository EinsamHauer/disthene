#!/bin/sh -e
if [ "$1" = "purge" ] ; then
	update-rc.d disthene remove >/dev/null
fi
