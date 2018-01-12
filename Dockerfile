FROM scratch
ADD ./target/linux-amd64/k-ray /
EXPOSE 8080
ENTRYPOINT ["/k-ray"]
