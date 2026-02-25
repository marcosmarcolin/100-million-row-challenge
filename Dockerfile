FROM php:8.5-cli-alpine

WORKDIR /app

RUN apk add --no-cache \
        git \
        icu-dev \
        $PHPIZE_DEPS \
    && docker-php-ext-install intl \
    && apk del --no-network $PHPIZE_DEPS \
    && git config --global --add safe.directory /app

COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

ENV COMPOSER_ALLOW_SUPERUSER=1