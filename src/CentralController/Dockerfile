FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY publish/ ./
RUN mkdir -p wwwroot/ota data
ENTRYPOINT ["dotnet", "CentralController.dll"]
