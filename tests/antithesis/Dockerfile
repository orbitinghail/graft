FROM alpine:3 AS base
COPY docker-compose.yaml /docker-compose.yaml

# replace `:latest` with the actual tag in the docker-compose.yaml
ARG TAG
RUN sed -i "s/:latest/:${TAG}/g" /docker-compose.yaml

FROM scratch
COPY --from=base /docker-compose.yaml /docker-compose.yaml
