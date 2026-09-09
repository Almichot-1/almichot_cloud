package build

// BuildStrategy represents the technology stack or build method detected.
type BuildStrategy string

const (
	StrategyDockerfile BuildStrategy = "DOCKERFILE"
	StrategyGo         BuildStrategy = "GO"
	StrategyNode       BuildStrategy = "NODE"
	StrategyPython     BuildStrategy = "PYTHON"
)

// BuildPlan specifies the instructions and synthetic Dockerfile for an application build.
type BuildPlan struct {
	Strategy          BuildStrategy
	DockerfilePath    string
	DockerfileContent string
	BaseImage         string
	ExposedPort       int
}

// Default synthetic Dockerfiles for zero-config buildpacks
const (
	DefaultGoDockerfile = `FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /server /app/server
EXPOSE 8080
ENTRYPOINT ["/app/server"]
`

	DefaultNodeDockerfile = `FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
EXPOSE 3000
ENTRYPOINT ["npm", "start"]
`

	DefaultPythonDockerfile = `FROM python:3.11-slim
WORKDIR /app
COPY requirements*.txt pyproject.toml* ./
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi
COPY . .
EXPOSE 8000
ENTRYPOINT ["python", "main.py"]
`
)
