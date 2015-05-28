#!/bin/sh -e
# Fakeroot and lein don't get along, so we set ownership after the fact.
chown -R root:root /usr/lib/disthene
chown root:root /usr/bin/disthene
chown disthene:disthene /var/log/disthene
chown -R disthene:disthene /etc/disthene
chown root:root /etc/init.d/disthene

if [ -x "/etc/init.d/disthene" ]; then
	update-rc.d disthene start 50 2 3 4 5 . stop 50 0 1 6 . >/dev/null
	invoke-rc.d disthene start || exit $?
fi
