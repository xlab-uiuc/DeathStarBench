#!/bin/bash

ADMIN_USER="admin"
ADMIN_PWD="admin"

TARGET_DB="rate-db"
READ_WRITE_ROLE="readWrite"

echo "Waiting for MongoDB to start..."
until mongo --eval "print('waited for connection')" > /dev/null 2>&1; do
  sleep 1
done
echo "MongoDB started"

# Create the admin user (will fail if the user already exists)
echo "Creating admin user..."
mongo admin --eval "db.createUser({user: '$ADMIN_USER', pwd: '$ADMIN_PWD', roles:[{role:'userAdminAnyDatabase',db:'admin'}]});"

# Grant readWrite role on the target database
echo "Granting readWrite role to $ADMIN_USER on $TARGET_DB database..."
mongo admin -u $ADMIN_USER -p $ADMIN_PWD --authenticationDatabase admin \
     --eval "db.grantRolesToUser('$ADMIN_USER', [{role: '$READ_WRITE_ROLE', db: '$TARGET_DB'}]);"

echo "Initialization script completed"
