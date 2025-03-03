FROM python:3-alpine

RUN apk update && apk add iptables && echo "net.ipv4.ip_forward=1"

CMD [ "/bin/sh" ] 