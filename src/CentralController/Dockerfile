FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY publish/ ./
RUN mkdir -p wwwroot/ota
ENTRYPOINT ["dotnet", "CentralController.dll"]
