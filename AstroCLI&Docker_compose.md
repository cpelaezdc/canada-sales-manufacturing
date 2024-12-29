# Commands

### Create an Astro project (go to directory project ot execute this command)
```bash
astro dev init
```
### To run Astro project locally:

```bash
astro dev start
```

### To restart and update with new changes:

```bash
astro dev restart
```

### To stop and mremove the Docker Compose containers and volumes:

```bash
astro dev stop
```

## Sources:

https://www.astronomer.io/docs/astro/cli/overview




# Commands for Docker compose (Data ware house)

### To run the Docker Compose environment:

```bash
docker compose up -d
```

### To stop and remove the Docker Compose containers:

```bash
docker compose down
```

### To restart the Docker Compose containers:

```bash
docker compose restart
```

### To stop and mremove the Docker Compose containers and volumes:

```bash
docker compose down -v
```

***Note***: Ensure you execute these commands from the same directory as your docker-compose.yml file.