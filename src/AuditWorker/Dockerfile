FROM mcr.microsoft.com/dotnet/runtime:9.0 AS runtime
WORKDIR /app
COPY publish/ ./
RUN mkdir -p data
ENTRYPOINT ["dotnet", "AuditWorker.dll"]
