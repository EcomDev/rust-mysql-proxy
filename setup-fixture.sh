#!/bin/bash

export MYSQL_PASSWORD=${MYSQL_PASSWORD:-tests}


function compose() {
  docker-compose -p async-mysql-proxy -f fixture/docker-compose.yml "$@"
}

function start_mysql {
   compose down >/dev/null 2>&1
   compose up -d mysql >/dev/null 2>&1
   last_status=1
   while [ $last_status -gt 0 ]
   do
      compose exec mysql mysql -uroot -p$MYSQL_PASSWORD -e "show databases" >/dev/null 2>&1
      last_status=$?
   done
}

if [[ "$1" == "stop" ]]
then
  compose down >/dev/null 2>&1
  exit
fi

if [[ "$1" == "log" ]]
then
  compose logs -f mysql
  exit
fi

if [[ "$1" == "exec" ]]
then
  shift
  compose exec mysql "$@"
  exit
fi

start_mysql
compose exec -T mysql mysql -u root -ptests < fixture/schema.sql >/dev/null 2>&1