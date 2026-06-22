FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ["FCG.Worker/FCG.Worker.csproj", "FCG.Worker/"]
COPY ["FCG.Application/FCG.Application.csproj", "FCG.Application/"]
COPY ["FCG.Infrastructure/FCG.Infrastructure.csproj", "FCG.Infrastructure/"]

RUN dotnet restore "FCG.Worker/FCG.Worker.csproj"

COPY . .

RUN dotnet build "FCG.Worker/FCG.Worker.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "FCG.Worker/FCG.Worker.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "FCG.Worker.dll"]