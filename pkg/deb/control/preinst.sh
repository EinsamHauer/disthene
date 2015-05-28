#!/bin/sh -e
USERNAME="disthene"
GROUPNAME="disthene"
getent group "$GROUPNAME" >/dev/null || groupadd -r "$GROUPNAME"
getent passwd "$USERNAME" >/dev/null || \
      useradd -r -g "$GROUPNAME" -d /usr/lib/disthene -s /bin/false \
      -c "Disthene metric storage daemon" "$USERNAME"
exit 0