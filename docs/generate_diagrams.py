import urllib.request
import os

system_architecture_mermaid = """
graph TD
    subgraph Host ["Host System"]
        Port3000["Port 3000 (Grafana UI)"]
        Port18083["Port 18083 (nginx1 EMQX Dashboard)"]
        Port18084["Port 18084 (nginx2 EMQX Dashboard)"]
        VolAudit[("Volume: ./data/audit.db")]
        VolDownloads[("Volume: ./downloads/")]
    end

    subgraph NetIoT ["iot_network (Device Zone)"]
        Device1["iotdevice1 (Simulator)"]
        Device2["iotdevice2 (Simulator)"]
        Device3["iotdevice3 (Simulator)"]
    end

    subgraph NetMqtt ["mqtt_network (Broker Zone)"]
        EMQX1["emqx1 (Clustered)"]
        EMQX2["emqx2 (Clustered)"]
    end

    subgraph NetController ["controller_network (Control Zone)"]
        Controller["centralcontroller"]
    end

    subgraph NetAudit ["audit_network (Audit Zone)"]
        subgraph ScaledAuditWorkers ["Scaled Audit Workers"]
            Worker1["auditworker-1"]
            Worker2["auditworker-2"]
        end
    end

    subgraph NetObs ["observability_network (Observability Zone)"]
        Loki["Loki"]
        Prometheus["Prometheus"]
        Grafana["Grafana"]
    end

    %% Bridging Components (connected to multiple networks)
    Vector["Vector (Telemetry Agent)"]

    subgraph NginxCluster ["Nginx Cluster"]
        Nginx1["nginx1"]
        Nginx2["nginx2"]
    end

    subgraph RabbitCluster ["RabbitMQ Cluster"]
        RabbitMQ1["rabbitmq1"]
        RabbitMQ2["rabbitmq2"]
        RabbitMQ3["rabbitmq3"]
    end

    %% Network Connections for Nginx
    Device1 & Device2 & Device3 <-->|iot_network| Nginx1 & Nginx2
    Nginx1 & Nginx2 <-->|mqtt_network| EMQX1 & EMQX2
    Nginx1 & Nginx2 <-->|controller_network| Controller

    %% Network Connections for RabbitMQ
    EMQX1 & EMQX2 <-->|mqtt_network| RabbitMQ1 & RabbitMQ2 & RabbitMQ3
    RabbitMQ1 & RabbitMQ2 & RabbitMQ3 <-->|audit_network| Worker1 & Worker2
    RabbitMQ1 & RabbitMQ2 & RabbitMQ3 <-->|audit_network| Vector

    %% Network Connections for Vector
    Vector <-->|observability_network| Loki & Prometheus

    %% Observability stack queries
    Loki & Prometheus <-->|observability_network| Grafana

    %% Host mappings
    Port3000 <--> Grafana
    Port18083 <--> Nginx1
    Port18084 <--> Nginx2
    Worker1 & Worker2 -->|Mount| VolAudit
    Device1 & Device2 & Device3 -->|Mount| VolDownloads

    style Nginx1 fill:#f9f,stroke:#333
    style Nginx2 fill:#f9f,stroke:#333
    style RabbitMQ1 fill:#ff9,stroke:#333
    style RabbitMQ2 fill:#ff9,stroke:#333
    style RabbitMQ3 fill:#ff9,stroke:#333
    style Vector fill:#9f9,stroke:#333,stroke-width:2px
    style Host fill:#ddd,stroke:#333,stroke-dasharray: 5 5
"""


logical_flow_mermaid = """
graph TD
    subgraph DeviceZone ["Device Zone"]
        IoT["IoT Devices 1, 2, 3"]
    end

    subgraph ProxyZone ["Proxy & Load Balancing"]
        Nginx["Nginx Cluster (2 nodes)"]
    end

    subgraph BrokerZone ["Message Broker Cluster"]
        EMQX["EMQX MQTT Cluster"]
        Rabbit["RabbitMQ Cluster"]
    end

    subgraph ControlZone ["Control Zone"]
        Controller["Central Controller"]
    end

    subgraph AuditZone ["Audit/Backend Zone"]
        Worker["Audit Workers 1, 2"]
        DB[("SQLite audit.db")]
    end

    subgraph ObsPipeline ["Observability Pipeline"]
        Vector["Vector Telemetry Agent"]
        Loki[("Grafana Loki Logs")]
        Prom[("Prometheus Metrics")]
        Grafana["Grafana Dashboards"]
    end

    %% Telemetry, Log, and Metric Flow
    IoT -->|1. Publish Telemetry/Logs/Metrics MQTT:1883| Nginx
    Nginx -->|2. Load Balance TCP| EMQX
    EMQX -->|3. Data Bridge| Rabbit
    Rabbit -->|4a. Consume Logs/Telemetry/Metrics| Vector
    Rabbit -->|4b. Consume Audit Events| Worker
    Worker -->|5. Persist| DB

    %% Vector Routing
    Vector -->|6a. Send Logs & Raw JSON Payloads| Loki
    Vector -->|6b. Send System Metrics & Extracted Gauges| Prom
    Loki -->|7. Query Data| Grafana
    Prom -->|7. Query Data| Grafana

    %% Control & Configuration Flow
    Controller -->|Publish Config/Control| Nginx
    Nginx -->|Load Balance| EMQX
    EMQX -->|Deliver Command| IoT
    IoT -->|Publish Command ACK| Nginx

    %% OTA Firmware Flow
    Controller -->|1. Publish OTA Notice via MQTT| Nginx
    Nginx -->|2. Route OTA Notice| EMQX
    EMQX -->|3. Deliver OTA Notice| IoT
    IoT -->|4. HTTP GET /ota/firmware.bin| Nginx
    Nginx -->|5. Proxy HTTP GET| Controller
    Controller -->|6. Serve Firmware Binary| Nginx
    Nginx -->|7. Deliver Firmware Binary| IoT
    IoT -->|8. Save to volume| Vol["downloads/ folder"]

    style IoT fill:#fcf,stroke:#333
    style Nginx fill:#f9f,stroke:#333
    style EMQX fill:#dfd,stroke:#333
    style Rabbit fill:#ff9,stroke:#333
    style Controller fill:#dff,stroke:#333
    style Worker fill:#dff,stroke:#333
    style Vector fill:#9f9,stroke:#333
"""

def generate_image(mermaid_code, output_path):
    print(f"Generating diagram for {output_path}...")
    url = "https://kroki.io/mermaid/png"
    headers = {
        "Content-Type": "text/plain",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    req = urllib.request.Request(url, data=mermaid_code.encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            image_data = response.read()
            with open(output_path, "wb") as f:
                f.write(image_data)
            print(f"Successfully wrote image to {output_path}")
    except urllib.error.HTTPError as e:
        print(f"Failed to generate {output_path}: {e}")
        try:
            error_body = e.read().decode('utf-8')
            print(f"Error details: {error_body}")
        except Exception:
            pass
    except Exception as e:
        print(f"Failed to generate {output_path}: {e}")

if __name__ == "__main__":
    os.makedirs("docs/images", exist_ok=True)
    generate_image(system_architecture_mermaid, "docs/images/system_architecture.png")
    generate_image(logical_flow_mermaid, "docs/images/logical_flow.png")
