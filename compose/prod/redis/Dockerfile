FROM redis/redis-stack-server:latest

COPY redis-stack.conf /redis-stack.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
