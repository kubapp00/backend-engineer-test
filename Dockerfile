FROM oven/bun:1
WORKDIR /usr/src/app
COPY package.json bun.lockb ./
RUN bun install
COPY . .
EXPOSE 3000
ENTRYPOINT ["bun", "run", "start"]