require('dotenv').config();
const express = require('express');
const path = require('path');
const cors = require('cors');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST", "PUT", "DELETE"]
    }
});
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Serve frontend static files from ../public
app.use(express.static(path.join(__dirname, '..', 'public')));

// MySQL connection setup (creates DB and tables automatically)
let dbPool;
async function initializeDatabase() {
    try {
        const connection = await mysql.createConnection({
            host: process.env.DB_HOST || 'localhost',
            user: process.env.DB_USER || 'root',
            password: process.env.DB_PASSWORD || '',
        });

        const dbName = process.env.DB_NAME || 'cta';
        await connection.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
        await connection.end();

        dbPool = mysql.createPool({
            host: process.env.DB_HOST || 'localhost',
            user: process.env.DB_USER || 'root',
            password: process.env.DB_PASSWORD || '',
            database: dbName,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0
        });

        // Drop old tables if they exist
        await dbPool.query("DROP TABLE IF EXISTS alarms, alarms_history, thresholds, telemetry_history, actuators, sensors, devices, `groups` CASCADE");

        // 1. CTA Table
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS cta (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nom VARCHAR(50) NOT NULL,
                etat VARCHAR(20) DEFAULT 'inactif',
                mode VARCHAR(20) DEFAULT 'Chauffage'
            )
        `);
        try { await dbPool.query(`ALTER TABLE cta ADD COLUMN etat VARCHAR(20) DEFAULT 'inactif'`); } catch(e) {}
        try { await dbPool.query(`ALTER TABLE cta ADD COLUMN mode VARCHAR(20) DEFAULT 'Chauffage'`); } catch(e) {}
        try { await dbPool.query(`ALTER TABLE cta ADD COLUMN consigne FLOAT DEFAULT 22`); } catch(e) {}
        try { await dbPool.query(`ALTER TABLE cta ADD COLUMN regulation_mode VARCHAR(10) DEFAULT 'auto'`); } catch(e) {}

        // 2. Temperature Table
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS cta_temperature (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cta_id INT,
                reprise FLOAT,
                soufflage FLOAT,
                salle FLOAT,
                chaud_aller FLOAT,
                chaud_retour FLOAT,
                froid_aller FLOAT,
                froid_retour FLOAT,
                vanne_ouverture INT DEFAULT 0,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cta_id) REFERENCES cta(id) ON DELETE CASCADE
            )
        `);
        // 3. Energie Table
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS energie (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cta_id INT,
                tension FLOAT,
                courant FLOAT,
                puissance FLOAT,
                energie_kwh FLOAT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cta_id) REFERENCES cta(id) ON DELETE CASCADE
            )
        `);

        // 4. Air Quality Table
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS air_quality (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cta_id INT,
                aqi INT,
                tvoc INT,
                eco2 INT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cta_id) REFERENCES cta(id) ON DELETE CASCADE
            )
        `);

        // 5. Alertes Table
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS alertes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cta_id INT,
                type VARCHAR(50),
                message TEXT,
                valeur FLOAT,
                niveau VARCHAR(20),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cta_id) REFERENCES cta(id) ON DELETE CASCADE
            )
        `);


        // 6b. Schedules Table (auto planning with mode + consigne)
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS schedules (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cta_id INT NOT NULL,
                name VARCHAR(100) NOT NULL,
                days VARCHAR(30) DEFAULT 'lun,mar,mer,jeu,ven,sam,dim',
                heure_on TIME NOT NULL,
                heure_off TIME NOT NULL,
                mode VARCHAR(20) DEFAULT 'Chauffage',
                consigne FLOAT DEFAULT 22,
                enabled TINYINT(1) DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cta_id) REFERENCES cta(id) ON DELETE CASCADE
            )
        `);

        // 7. Seuils Table
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS seuils (
                id INT AUTO_INCREMENT PRIMARY KEY,
                type VARCHAR(50),
                nom VARCHAR(50),
                min FLOAT,
                max FLOAT
            )
        `);

        // 8. Users Table (Keep existing)
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(150) NOT NULL,
                role ENUM('admin', 'visiteur') NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Seeding initial data
        const [ctaRows] = await dbPool.query("SELECT * FROM cta LIMIT 1");
        if (ctaRows.length === 0) {
            console.log('🌱 Seeding initial CTA data...');
            await dbPool.query("INSERT INTO cta (nom) VALUES ('CTA Centrale')");
            
            // Use INSERT IGNORE to avoid duplicate entry errors for the admin user
            await dbPool.query("INSERT IGNORE INTO users (username, password, role) VALUES ('admin', 'admin123', 'admin')");
            
            // Seed default thresholds
            await dbPool.query("INSERT IGNORE INTO seuils (type, nom, min, max) VALUES ('temp', 'salle', 18, 25)");
            await dbPool.query("INSERT IGNORE INTO seuils (type, nom, min, max) VALUES ('air', 'eco2', 0, 1000)");
            await dbPool.query("INSERT IGNORE INTO seuils (type, nom, min, max) VALUES ('humidite', 'humidite', 30, 70)");
            await dbPool.query("INSERT IGNORE INTO seuils (type, nom, min, max) VALUES ('filtre', 'pression_filtre', 0, 150)");
            
            console.log('✅ Base seeding complete.');
        }

        // 9. Device Config Table (ESP32 network configuration)
        await dbPool.query(`
            CREATE TABLE IF NOT EXISTS device_config (
                id INT AUTO_INCREMENT PRIMARY KEY,
                device_id INT,
                card_number INT DEFAULT 1,
                role VARCHAR(50) DEFAULT 'capteurs',
                device_name VARCHAR(100) NOT NULL,
                static_ip VARCHAR(20) DEFAULT '',
                gateway VARCHAR(20) DEFAULT '192.168.1.1',
                subnet VARCHAR(20) DEFAULT '255.255.255.0',
                dns VARCHAR(20) DEFAULT '8.8.8.8',
                wifi_ssid VARCHAR(100) DEFAULT '',
                wifi_password VARCHAR(100) DEFAULT '',
                last_seen TIMESTAMP NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        `);

        // Add card_number and role columns if they don't exist (migration)
        try { await dbPool.query(`ALTER TABLE device_config ADD COLUMN card_number INT DEFAULT 1`); } catch(e) {}
        try { await dbPool.query(`ALTER TABLE device_config ADD COLUMN role VARCHAR(50) DEFAULT 'capteurs'`); } catch(e) {}

        console.log('🚀 Database schema fully initialized with specialized CTA structure.');
    } catch (error) {
        console.error('❌ Error initializing database:', error);
    }
}

// MQTT Client Setup
const MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://broker.hivemq.com:1883';
const MQTT_TOPIC_TELEMETRY = 'cta/telemetry';
const MQTT_TOPIC_COMMANDS = 'cta/commands';

const mqttClient = mqtt.connect(MQTT_BROKER, {
    clientId: 'cta-backend-' + Math.random().toString(16).substr(2, 8),
    username: process.env.MQTT_USER, 
    password: process.env.MQTT_PASSWORD
});

mqttClient.on('connect', () => {
    console.log(`Connected to MQTT broker: ${MQTT_BROKER}`);
    // Subscribe to all cta topics using wildcard
    mqttClient.subscribe('cta/#', (err) => {
        if (!err) {
            console.log(`Subscribed to topic: cta/# (Listening for all telemetry variants)`);
        }
    });
    // Also support uppercase variants if needed
    mqttClient.subscribe('CTA/#');
});

mqttClient.on('message', async (topic, payload) => {
    const lowerTopic = topic.toLowerCase();

    // ── Vanne ESP32 status (cta/vanne/status) ────────────────────
    if (lowerTopic.includes('vanne/status')) {
        try {
            const data = JSON.parse(payload.toString());
            console.log(`[VANNE STATUS] ${JSON.stringify(data)}`);
            io.emit('vanne_status', {
                vanne_ouverture: data.vanne_ouverture ?? 0,
                consigne:        data.consigne ?? 22,
                reprise:         data.reprise ?? 0,
                regulation_auto: data.regulation_auto ?? true,
                timestamp:       new Date()
            });
        } catch(e) {
            console.error('[VANNE STATUS] Parse error:', e.message);
        }
        return;
    }

    // ── ENS160 : qualité d'air (cta/+/sensors) ───────────────────
    if (lowerTopic.includes('/sensors')) {
        try {
            const data = JSON.parse(payload.toString());
            console.log(`📥 MQTT Sensors [${topic}]:`, data);
            if (dbPool) {
                const [ctaRows] = await dbPool.query("SELECT id FROM cta LIMIT 1");
                if (ctaRows.length === 0) return;
                const ctaId = ctaRows[0].id;

                // Sauvegarder en base
                if (data.eco2 !== undefined || data.aqi !== undefined) {
                    await dbPool.query(
                        "INSERT INTO air_quality (cta_id, aqi, tvoc, eco2) VALUES (?, ?, ?, ?)",
                        [ctaId, data.aqi ?? null, data.tvoc ?? null, data.eco2 ?? null]
                    );
                }

                // Vérifier le seuil de température salle
                if (data.salle !== undefined) {
                    if (!global.activeAlarms) global.activeAlarms = {};
                    const [seuils] = await dbPool.query("SELECT * FROM seuils WHERE nom = 'salle'");
                    for (const seuil of seuils) {
                        const val = data.salle;
                        const alarmKey = `cta_${ctaId}_salle`;
                        let isBreached = false;
                        if (seuil.max !== null && val > seuil.max) isBreached = true;
                        if (seuil.min !== null && val < seuil.min) isBreached = true;

                        if (isBreached) {
                            if (!global.activeAlarms[alarmKey]) {
                                global.activeAlarms[alarmKey] = true;
                                await dbPool.query(
                                    "INSERT INTO alertes (cta_id, type, message, valeur, niveau) VALUES (?, ?, ?, ?, ?)",
                                    [ctaId, seuil.type, `Alerte Seuil: salle est à ${val}°C`, val, 'danger']
                                );
                                io.emit('alarm', {
                                    ctaId, type: seuil.type,
                                    message: `Alerte Seuil: salle est à ${val}°C`,
                                    severity: 'danger', value: val, created_at: new Date()
                                });
                            }
                        } else {
                            const hyst = 0.5;
                            const thresholdMax = parseFloat(seuil.max);
                            const thresholdMin = parseFloat(seuil.min);
                            let dangerous = false;
                            if (!isNaN(thresholdMax) && val > (thresholdMax - hyst)) dangerous = true;
                            if (!isNaN(thresholdMin) && val < (thresholdMin + hyst)) dangerous = true;
                            if (!dangerous && global.activeAlarms[alarmKey]) {
                                global.activeAlarms[alarmKey] = false;
                            }
                        }
                    }
                }

                // Envoyer au dashboard via Socket.io
                io.emit('telemetry', { ctaId, data, timestamp: new Date() });
            }
        } catch (error) {
            console.error('Failed to process sensors message:', error);
        }
        return;
    }

    // ── Autres capteurs : télémétrie (cta/telemetry) ─────────────
    if (lowerTopic.includes('tele') || lowerTopic.includes('télémé')) {
        try {
            const data = JSON.parse(payload.toString());
            console.log(`📥 MQTT Telemetry Received [${topic}]:`, data);
            
            if (dbPool) {
                // 1. Get the main CTA (assume ID 1)
                const [ctaRows] = await dbPool.query("SELECT id FROM cta LIMIT 1");
                if (ctaRows.length === 0) return;
                const ctaId = ctaRows[0].id;

                // 2. Forward reprise to valve ESP32 via MQTT
                if (data.reprise !== undefined && data.reprise > 0) {
                    mqttClient.publish('cta/1/reprise', JSON.stringify({ reprise: data.reprise }));
                }

                // 2b. Process Temperature Data (throttled to 1 record per 15 minutes)
                if (data.reprise !== undefined) {
                    if (!global.lastTempInsert) global.lastTempInsert = {};
                    const now = Date.now();
                    const FIFTEEN_MIN = 15 * 60 * 1000;
                    if (!global.lastTempInsert[ctaId] || (now - global.lastTempInsert[ctaId]) >= FIFTEEN_MIN) {
                        await dbPool.query(
                            "INSERT INTO cta_temperature (cta_id, reprise, soufflage, salle, chaud_aller, chaud_retour, froid_aller, froid_retour, vanne_ouverture) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            [ctaId, data.reprise, data.soufflage, data.salle, data.chaud_aller, data.chaud_retour, data.froid_aller, data.froid_retour, data.vanne_ouverture || 0]
                        );
                        global.lastTempInsert[ctaId] = now;
                        console.log(`[TEMP HISTORY] Saved temperature snapshot for CTA ${ctaId}`);
                    }
                }

                // 3. Process Energy Data
                if (data.tension !== undefined) {
                    await dbPool.query(
                        "INSERT INTO energie (cta_id, tension, courant, puissance, energie_kwh) VALUES (?, ?, ?, ?, ?)",
                        [ctaId, data.tension, data.courant, data.puissance, data.energie_kwh]
                    );
                }

                // 4. Process Air Quality Data
                if (data.aqi !== undefined || data.eco2 !== undefined) {
                    await dbPool.query(
                        "INSERT INTO air_quality (cta_id, aqi, tvoc, eco2) VALUES (?, ?, ?, ?)",
                        [ctaId, data.aqi, data.tvoc, data.eco2]
                    );
                }

                // 4b. CO2-based extractor automation (all modes)
                if (data.eco2 !== undefined && data.eco2 !== null) {
                    const co2 = parseInt(data.eco2);
                    if (!global.activeAlarms) global.activeAlarms = {};
                    if (!global.extracteurState) global.extracteurState = {};

                    if (co2 > 1000 && global.extracteurState[ctaId] !== 'on') {
                        global.extracteurState[ctaId] = 'on';
                        mqttClient.publish(`cta/${ctaId}/commands`, JSON.stringify({ actuatorType: 'extractor', value: 'on' }));
                        io.emit('extracteur_auto', { state: 'ON', co2, ctaId });
                        console.log(`[CO2 AUTO] ${co2} PPM > 1000 → Extracteur ON`);
                    } else if (co2 < 900 && global.extracteurState[ctaId] !== 'off') {
                        global.extracteurState[ctaId] = 'off';
                        mqttClient.publish(`cta/${ctaId}/commands`, JSON.stringify({ actuatorType: 'extractor', value: 'off' }));
                        io.emit('extracteur_auto', { state: 'OFF', co2, ctaId });
                        console.log(`[CO2 AUTO] ${co2} PPM < 900 → Extracteur OFF`);
                    }

                    const alarmKeyDanger = `cta_${ctaId}_co2_danger`;
                    if (co2 > 2000) {
                        if (!global.activeAlarms[alarmKeyDanger]) {
                            global.activeAlarms[alarmKeyDanger] = true;
                            console.log(`[CO2 ALARM] 🚨 CRITIQUE: eco2=${co2} PPM > 2000`);
                            await dbPool.query(
                                "INSERT INTO alertes (cta_id, type, message, valeur, niveau) VALUES (?, ?, ?, ?, ?)",
                                [ctaId, 'air', `ALARME CO₂ CRITIQUE: ${co2} PPM — Qualité d'air dangereuse!`, co2, 'danger']
                            );
                            io.emit('alarm', {
                                ctaId, type: 'air',
                                message: `ALARME CO₂ CRITIQUE: ${co2} PPM — Qualité d'air dangereuse!`,
                                severity: 'danger', value: co2, created_at: new Date()
                            });
                        }
                    } else if (co2 <= 1800 && global.activeAlarms[alarmKeyDanger]) {
                        global.activeAlarms[alarmKeyDanger] = false;
                        console.log(`[CO2 ALARM] ✅ RESET: eco2=${co2} PPM retombé sous 1800`);
                    }
                }

                // 5. Check Thresholds and Create Alerts (Anti-Spam)
                if (!global.activeAlarms) global.activeAlarms = {};
                
                const [seuils] = await dbPool.query("SELECT * FROM seuils");
                for (const seuil of seuils) {
                    const val = data[seuil.nom];
                    if (val !== undefined) {
                        let isBreached = false;
                        if (seuil.max !== null && val > seuil.max) isBreached = true;
                        if (seuil.min !== null && val < seuil.min) isBreached = true;

                        const alarmKey = `cta_${ctaId}_${seuil.nom}`;

                        if (isBreached) {
                            if (!global.activeAlarms[alarmKey]) {
                                global.activeAlarms[alarmKey] = true;
                                console.log(`[ALARM] 🚨 TRIGGERED: ${seuil.nom} = ${val} (Max: ${seuil.max})`);
                                
                                await dbPool.query(
                                    "INSERT INTO alertes (cta_id, type, message, valeur, niveau) VALUES (?, ?, ?, ?, ?)",
                                    [ctaId, seuil.type, `Alerte Seuil: ${seuil.nom} est à ${val}`, val, 'danger']
                                );
                                
                                io.emit('alarm', {
                                    ctaId,
                                    type: seuil.type,
                                    message: `Alerte Seuil: ${seuil.nom} est à ${val}`,
                                    severity: 'danger',
                                    value: val,
                                    created_at: new Date()
                                });
                            }
                        } else {
                            // Hysteresis: Only reset if the value is SAFELY back in the normal zone
                            const hyst = 0.5;
                            const thresholdMax = parseFloat(seuil.max);
                            const thresholdMin = parseFloat(seuil.min);
                            
                            let dangerous = false;
                            if (!isNaN(thresholdMax) && val > (thresholdMax - hyst)) dangerous = true;
                            if (!isNaN(thresholdMin) && val < (thresholdMin + hyst)) dangerous = true;
                            
                            if (!dangerous && global.activeAlarms[alarmKey]) {
                                console.log(`[ALARM] ✅ RESET: ${seuil.nom} is now safe at ${val} (Safely away from ${thresholdMin}-${thresholdMax})`);
                                global.activeAlarms[alarmKey] = false;
                            }
                        }
                    }
                }

                // 6. Broadcast live data via Socket.io
                io.emit('telemetry', {
                    ctaId,
                    data,
                    timestamp: new Date()
                });
            }
        } catch (error) {
            console.error('Failed to process MQTT message:', error);
        }
    }
});

// ═══════════════════════════════════════════════════════════
// REST API Routes
// ═══════════════════════════════════════════════════════════

// Health check (replaces connect_test.php)
app.get('/api/health', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ status: 'error', message: 'Database not initialized' });
        await dbPool.query('SELECT 1');
        res.json({ status: 'ok', message: 'Connected to database', database: process.env.DB_NAME || 'cta' });
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

// Login verification
app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;
    
    if (!username || !password) {
        return res.status(400).json({ error: 'Missing credentials' });
    }

    try {
        if (!dbPool) return res.status(500).json({ error: 'Database not initialized' });
        
        const [rows] = await dbPool.query('SELECT * FROM users WHERE username = ? LIMIT 1', [username]);
        if (rows.length === 0) {
            return res.status(401).json({ error: 'Utilisateur introuvable' });
        }

        const user = rows[0];
        
        // Comparing plaintext password for ease of migration (in production: use bcrypt.compare)
        if (user.password !== password) {
            return res.status(401).json({ error: 'Mot de passe incorrect' });
        }

        // Authentication success
        res.json({
            success: true,
            user: {
                id: user.id,
                username: user.username,
                role: user.role
            }
        });

    } catch (error) {
        console.error('Login query error:', error);
        res.status(500).json({ error: 'Erreur interne du serveur' });
    }
});

// ─── IoT Structure Routes ──────────────────────────

// GET all CTA (as groups for compatibility)
app.get('/api/groups', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query('SELECT id, nom as name FROM cta');
        res.json(rows);
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// GET CTA by ID (as device for compatibility)
app.get('/api/groups/:id/devices', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query('SELECT id, nom as label FROM cta WHERE id = ?', [req.params.id]);
        res.json(rows);
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// GET latest telemetry for a CTA
app.get('/api/devices/:id/telemetry', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        
        const [temps] = await dbPool.query('SELECT * FROM cta_temperature WHERE cta_id = ? ORDER BY timestamp DESC LIMIT 1', [req.params.id]);
        const [energy] = await dbPool.query('SELECT * FROM energie WHERE cta_id = ? ORDER BY timestamp DESC LIMIT 1', [req.params.id]);
        const [air] = await dbPool.query('SELECT * FROM air_quality WHERE cta_id = ? ORDER BY timestamp DESC LIMIT 1', [req.params.id]);
        
        // Merge into a format the frontend expects (or a new unified format)
        res.json({
            temperature: temps[0] || {},
            energy: energy[0] || {},
            air_quality: air[0] || {}
        });
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// ─── Alarms Routes ───────────────────────────

// GET temperature history for a CTA
app.get('/api/history/temperatures', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const limit = parseInt(req.query.limit) || 30;
        const [rows] = await dbPool.query(`
            SELECT * FROM cta_temperature 
            ORDER BY timestamp DESC 
            LIMIT ?
        `, [limit]);
        res.json(rows);
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// GET air quality history
app.get('/api/history/air-quality', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const limit = parseInt(req.query.limit) || 100;
        const [rows] = await dbPool.query(
            'SELECT * FROM air_quality ORDER BY timestamp DESC LIMIT ?',
            [limit]
        );
        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET active alerts
app.get('/api/alarms/active', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query(`
            SELECT a.*, c.nom as cta_label
            FROM alertes a
            JOIN cta c ON a.cta_id = c.id
            ORDER BY a.timestamp DESC
            LIMIT 50
        `);
        res.json(rows);
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// Resolve an alert (mock for now as there's no is_resolved in the new SQL)
app.put('/api/alarms/:id/resolve', async (req, res) => {
    try {
        res.json({ success: true, message: "Alert acknowledged" });
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// DELETE all alerts
app.delete('/api/alarms/all', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        await dbPool.query("DELETE FROM alertes");
        
        // Reset anti-spam so it can re-trigger if conditions are still bad
        global.activeAlarms = {};
        console.log('🗑️ Alerts cleared and anti-spam reset.');
        
        res.json({ success: true, message: "All alerts cleared" });
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// --- NEW: History & Latest Telemetry Routes ---
app.get('/api/history/energy', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query(
            "SELECT puissance as value, timestamp as label FROM energie ORDER BY timestamp DESC LIMIT 20"
        );
        res.json(rows.reverse()); // Chronological order
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/energy/daily', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query(`
            SELECT DATE(timestamp) as date, SUM(energie_kwh) as total_kwh
            FROM energie
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            LIMIT 7
        `);
        res.json(rows.reverse());
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/energy/realtime', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const limit = parseInt(req.query.limit) || 60;
        const [rows] = await dbPool.query(
            "SELECT energie_kwh, puissance, tension, courant, timestamp FROM energie ORDER BY timestamp DESC LIMIT ?",
            [limit]
        );
        res.json(rows.reverse());
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/telemetry/latest', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [ctas] = await dbPool.query("SELECT * FROM cta");
        const results = [];
        
        for (const cta of ctas) {
            const [t] = await dbPool.query("SELECT * FROM cta_temperature WHERE cta_id = ? ORDER BY id DESC LIMIT 1", [cta.id]);
            const [e] = await dbPool.query("SELECT * FROM energie WHERE cta_id = ? ORDER BY id DESC LIMIT 1", [cta.id]);
            const [a] = await dbPool.query("SELECT * FROM air_quality WHERE cta_id = ? ORDER BY id DESC LIMIT 1", [cta.id]);
            
            // KPI: Delta-T soufflage vs reprise (°C)
            const deltaT = (t[0] && t[0].soufflage != null && t[0].reprise != null)
                ? parseFloat((t[0].soufflage - t[0].reprise).toFixed(1)) : null;

            // KPI: COP estimate — thermal power / electrical power (approx flow 0.5 kg/s, Cp air = 1.005 kJ/kg·K)
            const thermalKW = (deltaT !== null) ? Math.abs(deltaT) * 0.5 * 1.005 : null;
            const cop = (thermalKW && e[0] && e[0].puissance > 50)
                ? parseFloat((thermalKW * 1000 / e[0].puissance).toFixed(2)) : null;

            // KPI: SFP — Specific Fan Power (W per m³/h), fan ~30% of load, nominal flow 3600 m³/h
            const sfp = (e[0] && e[0].puissance > 0)
                ? parseFloat((e[0].puissance * 0.3 / 3600).toFixed(3)) : null;

            results.push({
                name: cta.nom,
                status: cta.etat || 'inactif',
                mode: cta.mode || 'Chauffage',
                consigne: cta.consigne != null ? parseFloat(cta.consigne) : 22,
                regulation_mode: cta.regulation_mode || 'auto',
                temperature: t[0] ? t[0].salle : null,
                reprise: t[0] ? t[0].reprise : null,
                soufflage: t[0] ? t[0].soufflage : null,
                chaud_aller: t[0] ? t[0].chaud_aller : null,
                chaud_retour: t[0] ? t[0].chaud_retour : null,
                froid_aller: t[0] ? t[0].froid_aller : null,
                froid_retour: t[0] ? t[0].froid_retour : null,
                vanne_ouverture: t[0] ? t[0].vanne_ouverture : 0,
                eco2: a[0] ? a[0].eco2 : null,
                aqi: a[0] ? a[0].aqi : null,
                tvoc: a[0] ? a[0].tvoc : null,
                puissance: e[0] ? e[0].puissance : null,
                energie_kwh: e[0] ? e[0].energie_kwh : null,
                tension: e[0] ? e[0].tension : null,
                courant: e[0] ? e[0].courant : null,
                delta_t: deltaT,
                cop: cop,
                sfp: sfp,
                last_seen: t[0] ? t[0].timestamp : null
            });
        }
        res.json(results);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ─── Commands Route ──────────────────────────

app.post('/api/commands', async (req, res) => {
    const { deviceId, actuatorType, value } = req.body;
    
    if (!deviceId || !actuatorType) {
        return res.status(400).json({ error: 'Missing deviceId or actuatorType' });
    }

    try {
        // ✅ Correction du TOPIC pour correspondre à l'ESP32 : cta/ID/commands
        const topic = `cta/${deviceId}/commands`;
        
        // ✅ Correction du FORMAT pour correspondre à l'ESP32 : { "actuatorType": "...", "value": ... }
        const payload = JSON.stringify({ 
            actuatorType: actuatorType, 
            value: value 
        });

        mqttClient.publish(topic, payload, async (err) => {
            if (err) {
                console.error('[MQTT] Publish Error:', err);
                return res.status(500).json({ error: 'MQTT Publish failed' });
            }
            // Persister le mode en base quand une commande mode est envoyée
            if (actuatorType === 'mode' && value && dbPool) {
                await dbPool.query('UPDATE cta SET mode = ? WHERE id = ?', [value, deviceId]).catch(() => {});
            }
            if (actuatorType === 'consigne' && value !== undefined && dbPool) {
                await dbPool.query('UPDATE cta SET consigne = ? WHERE id = ?', [parseFloat(value), deviceId]).catch(() => {});
            }
            if (actuatorType === 'regulation_mode' && value && dbPool) {
                await dbPool.query('UPDATE cta SET regulation_mode = ? WHERE id = ?', [value, deviceId]).catch(() => {});
            }
            console.log(`[MQTT] Command Sent → ${topic}: ${payload}`);
            res.json({ success: true, message: `Command sent to ${topic}` });
        });
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// ─── Equipment Status Route ──────────────────────────────────
// Met à jour le champ etat (actif/inactif) dans la table cta
app.put('/api/equipments/status', async (req, res) => {
    const { ctaSlug, status } = req.body;
    if (!ctaSlug || !status) {
        return res.status(400).json({ error: 'Missing ctaSlug or status' });
    }
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        // Convertit le slug (ex: "salle-polyvalente") vers le nom DB (ex: "Salle Polyvalente")
        const [result] = await dbPool.query(
            `UPDATE cta SET etat = ? WHERE LOWER(REPLACE(nom, ' ', '-')) = ?`,
            [status, ctaSlug.toLowerCase()]
        );
        if (result.affectedRows === 0) {
            // Fallback: met à jour le premier CTA (id=1) si le slug ne correspond pas
            await dbPool.query('UPDATE cta SET etat = ? WHERE id = 1', [status]);
        }
        console.log(`[DB] CTA etat mis à jour → slug=${ctaSlug} etat=${status}`);
        res.json({ success: true, ctaSlug, status });
    } catch (err) {
        console.error('[DB] Erreur update etat:', err.message);
        res.status(500).json({ error: err.message });
    }
});

// ═══════════════════════════════════════════════════════════
// Schedule (Planification) Routes
// ═══════════════════════════════════════════════════════════

// GET all schedules, optionally filtered by ?cta_id=N
app.get('/api/schedules', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const ctaId = req.query.cta_id;
        const [rows] = ctaId
            ? await dbPool.query('SELECT * FROM schedules WHERE cta_id = ? ORDER BY heure_on', [ctaId])
            : await dbPool.query('SELECT * FROM schedules ORDER BY heure_on');
        const result = rows.map(s => ({ ...s, isActive: !!(global.activeSchedules?.[s.id]) }));
        res.json(result);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST create a schedule
app.post('/api/schedules', async (req, res) => {
    const { cta_id, name, days, heure_on, heure_off, mode, consigne } = req.body;
    if (!cta_id || !name || !heure_on || !heure_off) {
        return res.status(400).json({ error: 'cta_id, name, heure_on, heure_off are required' });
    }
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [result] = await dbPool.query(
            'INSERT INTO schedules (cta_id, name, days, heure_on, heure_off, mode, consigne) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [cta_id, name, days || 'lun,mar,mer,jeu,ven,sam,dim', heure_on, heure_off, mode || 'Chauffage', consigne || 22]
        );
        res.json({ success: true, id: result.insertId });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// PUT update a schedule (also handles enable/disable toggle)
app.put('/api/schedules/:id', async (req, res) => {
    const sid = parseInt(req.params.id);
    const { name, days, heure_on, heure_off, mode, consigne, enabled } = req.body;
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        await dbPool.query(
            `UPDATE schedules SET
                name = COALESCE(?, name),
                days = COALESCE(?, days),
                heure_on = COALESCE(?, heure_on),
                heure_off = COALESCE(?, heure_off),
                mode = COALESCE(?, mode),
                consigne = COALESCE(?, consigne),
                enabled = COALESCE(?, enabled)
            WHERE id = ?`,
            [name ?? null, days ?? null, heure_on ?? null, heure_off ?? null,
             mode ?? null, consigne ?? null, enabled !== undefined ? (enabled ? 1 : 0) : null, sid]
        );
        // If disabled and was active, end the schedule
        if ((enabled === false || enabled === 0) && global.activeSchedules?.[sid]) {
            global.activeSchedules[sid] = false;
            const [rows] = await dbPool.query('SELECT * FROM schedules WHERE id = ?', [sid]);
            if (rows.length > 0) {
                mqttClient.publish(`cta/${rows[0].cta_id}/commands`, JSON.stringify({
                    actuatorType: 'schedule_end', value: { scheduleId: sid }
                }));
                io.emit('schedule_deactivated', { scheduleId: sid, name: rows[0].name, ctaId: rows[0].cta_id });
            }
        }
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// DELETE a schedule
app.delete('/api/schedules/:id', async (req, res) => {
    const sid = parseInt(req.params.id);
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        if (global.activeSchedules?.[sid]) {
            const [rows] = await dbPool.query('SELECT * FROM schedules WHERE id = ?', [sid]);
            if (rows.length > 0) {
                global.activeSchedules[sid] = false;
                mqttClient.publish(`cta/${rows[0].cta_id}/commands`, JSON.stringify({
                    actuatorType: 'schedule_end', value: { scheduleId: sid }
                }));
                io.emit('schedule_deactivated', { scheduleId: sid, name: rows[0].name, ctaId: rows[0].cta_id });
            }
        }
        await dbPool.query('DELETE FROM schedules WHERE id = ?', [sid]);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ═══════════════════════════════════════════════════════════
// Network Config Routes (ESP32 IP management)
// ═══════════════════════════════════════════════════════════

// GET all ESP device gs
app.get('/api/network/devices', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query('SELECT * FROM device_config ORDER BY id');
        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET single ESP device config
app.get('/api/network/devices/:id', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query('SELECT * FROM device_config WHERE id = ?', [req.params.id]);
        if (rows.length === 0) return res.status(404).json({ error: 'Device not found' });
        res.json(rows[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST create a new ESP device config
app.post('/api/network/devices', async (req, res) => {
    const { device_id, card_number, role, device_name, static_ip, gateway, subnet, dns, wifi_ssid, wifi_password } = req.body;
    if (!device_name) return res.status(400).json({ error: 'device_name is required' });
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [result] = await dbPool.query(
            'INSERT INTO device_config (device_id, card_number, role, device_name, static_ip, gateway, subnet, dns, wifi_ssid, wifi_password) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [device_id || null, card_number || 1, role || 'capteurs', device_name, static_ip || '', gateway || '192.168.1.1', subnet || '255.255.255.0', dns || '8.8.8.8', wifi_ssid || '', wifi_password || '']
        );
        res.json({ success: true, id: result.insertId });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// PUT update ESP device config
app.put('/api/network/devices/:id', async (req, res) => {
    const { device_id, card_number, role, device_name, static_ip, gateway, subnet, dns, wifi_ssid, wifi_password } = req.body;
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        await dbPool.query(
            `UPDATE device_config SET
                device_id = COALESCE(?, device_id),
                card_number = COALESCE(?, card_number),
                role = COALESCE(?, role),
                device_name = COALESCE(?, device_name),
                static_ip = COALESCE(?, static_ip),
                gateway = COALESCE(?, gateway),
                subnet = COALESCE(?, subnet),
                dns = COALESCE(?, dns),
                wifi_ssid = COALESCE(?, wifi_ssid),
                wifi_password = COALESCE(?, wifi_password),
                updated_at = NOW()
            WHERE id = ?`,
            [device_id ?? null, card_number ?? null, role ?? null, device_name ?? null,
             static_ip ?? null, gateway ?? null, subnet ?? null, dns ?? null,
             wifi_ssid ?? null, wifi_password ?? null, req.params.id]
        );
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// DELETE ESP device config
app.delete('/api/network/devices/:id', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        await dbPool.query('DELETE FROM device_config WHERE id = ?', [req.params.id]);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST apply network config to ESP via MQTT
app.post('/api/network/devices/:id/apply', async (req, res) => {
    try {
        if (!dbPool) return res.status(500).json({ error: 'DB not initialized' });
        const [rows] = await dbPool.query('SELECT * FROM device_config WHERE id = ?', [req.params.id]);
        if (rows.length === 0) return res.status(404).json({ error: 'Device not found' });

        const cfg = rows[0];
        const deviceId = cfg.device_id || 1;
        const topic = `cta/${deviceId}/commands/${cfg.role || 'temperatures'}`;
        const payload = JSON.stringify({
            actuatorType: 'network_config',
            value: {
                static_ip: cfg.static_ip,
                gateway: cfg.gateway,
                subnet: cfg.subnet,
                dns: cfg.dns,
                wifi_ssid: cfg.wifi_ssid,
                wifi_password: cfg.wifi_password
            }
        });

        mqttClient.publish(topic, payload, async (err) => {
            if (err) {
                console.error('[MQTT] Network config publish error:', err);
                return res.status(500).json({ error: 'MQTT publish failed' });
            }
            // Update last_seen timestamp
            await dbPool.query('UPDATE device_config SET last_seen = NOW() WHERE id = ?', [cfg.id]);
            console.log(`[NETWORK CFG] Sent to ${topic}:`, payload);
            res.json({ success: true, message: `Config envoyée à ESP (topic: ${topic})` });
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ─── 404 for unknown API routes (prevents hanging requests) ──
app.use('/api', (req, res) => {
    res.status(404).json({ error: 'API route not found' });
});

// ─── SPA Fallback: serve index.html for unmatched non-API routes ──
app.get('/{*path}', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
});

// ─── Scheduler: checks active schedules every minute ─────
function startScheduler() {
    if (!global.activeSchedules) global.activeSchedules = {};
    const DAY_FR = ['dim', 'lun', 'mar', 'mer', 'jeu', 'ven', 'sam'];

    async function checkSchedules() {
        if (!dbPool) return;
        const now = new Date();
        const today = DAY_FR[now.getDay()];
        const currentTime = String(now.getHours()).padStart(2, '0') + ':' +
                            String(now.getMinutes()).padStart(2, '0') + ':00';
        try {
            const [schedules] = await dbPool.query('SELECT * FROM schedules WHERE enabled = 1');
            for (const s of schedules) {
                const days = s.days ? s.days.split(',') : [];
                const inTime = days.includes(today) && currentTime >= s.heure_on && currentTime < s.heure_off;
                const wasActive = !!global.activeSchedules[s.id];

                if (inTime && !wasActive) {
                    global.activeSchedules[s.id] = true;
                    await dbPool.query('UPDATE cta SET mode = ? WHERE id = ?', [s.mode, s.cta_id]).catch(() => {});
                    mqttClient.publish(`cta/${s.cta_id}/commands`, JSON.stringify({
                        actuatorType: 'schedule_start',
                        value: { mode: s.mode, consigne: s.consigne, scheduleId: s.id, name: s.name }
                    }));
                    console.log(`[SCHEDULER] ▶ "${s.name}" started → mode=${s.mode} consigne=${s.consigne}`);
                    io.emit('schedule_activated', {
                        scheduleId: s.id, name: s.name, ctaId: s.cta_id,
                        mode: s.mode, consigne: s.consigne,
                        heure_off: s.heure_off
                    });
                } else if (!inTime && wasActive) {
                    global.activeSchedules[s.id] = false;
                    mqttClient.publish(`cta/${s.cta_id}/commands`, JSON.stringify({
                        actuatorType: 'schedule_end', value: { scheduleId: s.id }
                    }));
                    console.log(`[SCHEDULER] ⏹ "${s.name}" ended → returning to manual`);
                    io.emit('schedule_deactivated', {
                        scheduleId: s.id, name: s.name, ctaId: s.cta_id
                    });
                }
            }
        } catch (err) {
            console.error('[SCHEDULER] Error:', err.message);
        }
    }

    checkSchedules();
    setInterval(checkSchedules, 60000);
    console.log('⏰ Schedule checker started (every 60s)');
}

// ─── Publie la reprise vers la vanne ESP32 toutes les 5s ──────
function startReprisePublisher() {
    async function publishReprise() {
        if (!dbPool) return;
        try {
            const [rows] = await dbPool.query(
                'SELECT reprise FROM cta_temperature WHERE reprise IS NOT NULL AND reprise > 0 ORDER BY id DESC LIMIT 1'
            );
            if (rows.length > 0 && rows[0].reprise > 0) {
                const payload = JSON.stringify({ reprise: rows[0].reprise });
                mqttClient.publish('cta/1/reprise', payload);
                console.log(`[REPRISE] Publié → cta/1/reprise: ${payload}`);
            }
        } catch (err) {
            console.error('[REPRISE] Erreur:', err.message);
        }
    }

    setInterval(publishReprise, 5000);
    console.log('🌡️  Reprise publisher started (every 5s)');
}

// Start Express Server
httpServer.listen(PORT, async () => {
    console.log(`🚀 CTA Backend & WebSocket Server running on port ${PORT}`);
    await initializeDatabase();
    startScheduler();
    startReprisePublisher();
});
