#!/bin/bash

ram_total=$(free | grep -i mem | awk '{ print $2 }' )
ram_consumed=$(free | grep -i mem | awk '{ print $3 }')
used_ram_ratio=$(bc <<< "scale=2;$ram_consumed/$ram_total *100" | awk -F. '{ print $1 }')

if [ $used_ram_ratio -gt 30 ]
then
mail -s "Consumo de memória acima do limite" mbrugnaroto@gmail.com<<del
Memory consumption is above the threshold that was specified. Currently consumption and $(free -h | grep -i mem | awk '{ print $3 }')
del
fi
