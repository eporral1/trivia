CHAT BATTLE - YouTube Live Chat (BLUE / RED)

REQUISITOS
- Windows 10/11
- Python 3.10+
- API Key de Google Cloud con YouTube Data API v3 habilitada
- Directo activo con el chat habilitado

INSTALACIÓN
1) Descomprimí la carpeta.
2) Abrí CMD dentro de la carpeta.
3) Ejecutá:

   py -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. stream_list.proto
   python server.py

4) Abrí http://127.0.0.1:8080
5) Ingresá API Key y URL del directo y presioná CONECTAR AL LIVE.
6) En el chat escribí BLUE o RED.

OBS / FORMATO VERTICAL
- Agregá una fuente Navegador con URL http://127.0.0.1:8080.
- Configurá la fuente en 540 x 960 (9:16), o 1080 x 1920.
- La página está armada en tres columnas: el escenario vertical queda en el centro; la conexión y los usuarios ficticios quedan a la izquierda y los datos auxiliares a la derecha.
- Para transmitir solo el juego, recortá la fuente Navegador al rectángulo vertical central (9:16). No incluyas los paneles laterales.
- Si preferís que la ventana completa del navegador sea el lienzo de OBS, ajustá el recorte a la columna central; los paneles laterales son controles y no forman parte del diseño del stream.

REGLAS DEL JUEGO
- BLUE y RED crean una bolita.
- El primer equipo elegido por un usuario queda bloqueado hasta reiniciar la ronda. Un comando del otro color se ignora.
- Cada usuario puede tener hasta 5 bolitas vivas simultáneamente. Cuando pierde una, puede crear otra con un nuevo mensaje de su equipo.
- Cada bolita tiene 3 vidas. Los choques entre equipos descuentan una vida; el daño al mismo par de bolitas tiene un breve intervalo para evitar que pierdan todas las vidas instantáneamente.
- La zona se reduce cada 30 segundos. Las bolitas que quedan fuera pierden una vida en cada reducción.
- Cuando una bolita es eliminada se muestra el nombre de su dueño y, si corresponde, quién la eliminó.
- La ronda termina cuando un equipo se queda sin bolitas. Usá REINICIAR RONDA para comenzar otra.
- Cada bolita muestra los primeros 3 caracteres del nombre del usuario. El contorno conserva el color identificador del usuario.
- El ranking muestra bolitas vivas, vidas restantes, eliminaciones y bolitas creadas por usuario.

USUARIOS FICTICIOS
- Los controles de prueba están en el panel lateral, fuera del encuadre vertical.
- Cada usuario ficticio tiene un color identificador fijo y botones BLUE / RED.
- Al pulsarlos se simula un mensaje entrante local; no se envía nada a YouTube.
- Respetan el bloqueo de equipo y el máximo de 5 bolitas vivas.

NOTAS TÉCNICAS
- videos.list obtiene activeLiveChatId a partir de la URL del directo.
- streamList usa gRPC server-streaming y API Key en el header x-goog-api-key.
- El backend local reenvía por WebSocket únicamente comandos BLUE / RED.
- Las físicas, animaciones, vidas, ranking y usuarios ficticios se procesan en el navegador; no se consulta ningún servicio adicional por fotograma.
- No pegues ni compartas tu API Key con nadie. Se ingresa en la página local y se envía al backend local.
