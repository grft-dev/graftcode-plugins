FROM nats:2.10

EXPOSE 4222 8222

# Enable JetStream and require guest/guest auth.
CMD ["-js", "--user", "guest", "--pass", "guest", "-m", "8222"]
