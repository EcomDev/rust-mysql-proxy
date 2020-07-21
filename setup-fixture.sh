#!/bin/bash

function compose() {
  docker-compose -p async-mysql-proxy -f fixture/docker-compose.yml $@
}

compose up -d
sleep 10

compose exec -T mysql mysql -u root -ptests < fixture/schema.sql