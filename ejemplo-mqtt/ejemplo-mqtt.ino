// Dependencias
#include <Adafruit_DHT_Particle.h>
#include <MQTT.h>

// REF: https://openhomeautomation.net/cloud-data-logger-particle-photon/

// Configuración del sensor DHT
#define DHTPIN 2
#define DHTTYPE DHT22

// Acceso al sensor DHT
DHT dht(DHTPIN, DHTTYPE);

// Configuración del cliente MQTT
MQTT client("192.168.1.100", 1883, callback);

// Se llama cuando se recibe un mensaje MQTT
void callback(char* topic, byte* payload, unsigned int length) 
{
}

void setup() {

    // Apagar el LED principal
    RGB.control(true); 
    RGB.color(0, 0, 0);
    
    // Activar el LED azul
    pinMode(D7, OUTPUT);

    // Iniciar el sensor DHT
    dht.begin();

    // Conectar al servidor MQTT
    client.connect("kit-02");
}

void loop() {
    
    // Leer los datos del sensor
    float temperature = dht.getTempCelcius();
    float humidity = dht.getHumidity();

    // Si estamos conectados al MQTT, enviar los datos
    if (client.isConnected())
    {
        // Formato: TOPIC JSON -> mediciones { "temperatura": 23.200001, "humedad": 48.000000 }
        client.publish("mediciones", "{ "  
            "\"temperatura\": " + String(temperature) + ", " 
            "\"humedad\": "  + String(humidity) + " "
        "}");

        // Parpadear el LED azul
        digitalWrite(D7, HIGH);
        delay(200);
        digitalWrite(D7, LOW);
        delay(4800);

        // La librería exige esta llamada al final del loop
        client.loop();
    }
}
