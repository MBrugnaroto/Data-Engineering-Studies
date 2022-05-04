#!/bin/bash

response_http=$(curl --write-out %{http_code} --silent --output /dev/null http://localhost)

if [ $response_http -ne 200 ]
then
mail -s "There is a problem on the server" brugnaroto@gmail.com<<del
There was a problem on the server and users stopped accessing web content.
del
fi
