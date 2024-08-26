const axios = require('axios');
const sql = require('mssql');

const dbConfig = {
    user: 'sa',
    password: 'powerdigitspassword',
    server: 'localhost',
    database: 'PowerDigits',
    options: {
        encrypt: false,
        enableArithAbort: true,
    }
};

const year = '2023';
const apiKey = 'EoLeorvh0YUDcFqt7KmceJDbA0J5p8H4VBj/936S4GxdnWOM5d1oPgfL1WJoxbxc';
const apiGamesEndpoint = `https://api.collegefootballdata.com/games?year=${year}&seasonType=regular`;
const apiTeamsEndpoint = `https://api.collegefootballdata.com/teams?conference=`;
const apiConferencesEndpoint = `https://api.collegefootballdata.com/conferences`;
const apiCalendarEndpoint = `https://api.collegefootballdata.com/calendar?year=${year}`;

let pool;

async function openConnection() {
    try {
        pool = await sql.connect(dbConfig);
        console.log('Connection established');
    } catch (error) {
        console.error('Error establishing connection:', error);
        throw error;
    }
}

async function closeConnection() {
    try {
        await pool.close();
        console.log('Connection closed');
    } catch (error) {
        console.error('Error closing connection:', error);
    }
}

async function truncateTables() {
    try {
        await pool.request().query`TRUNCATE TABLE games`;
        await pool.request().query`TRUNCATE TABLE teams`;
        await pool.request().query`TRUNCATE TABLE locations`;
        await pool.request().query`TRUNCATE TABLE conferences`;
        await pool.request().query`TRUNCATE TABLE calendar`;
        console.log('Tables truncated');
    } catch (error) {
        console.error('Error truncating tables:', error);
    }
}

async function createTables() {
    try {
        const createConferencesTable = `
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'conferences')
            BEGIN
                CREATE TABLE conferences (
                    id INT,
                    name VARCHAR(255),
                    short_name VARCHAR(100),
                    abbreviation VARCHAR(10),
                    classification VARCHAR(50)
                );
            END
        `;

        const createLocationsTable = `
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'locations')
            BEGIN
                CREATE TABLE locations (
                    venue_id INT,
                    name VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(100),
                    zip VARCHAR(20),
                    country_code VARCHAR(10),
                    timezone VARCHAR(50),
                    latitude FLOAT,
                    longitude FLOAT,
                    elevation FLOAT,
                    capacity INT,
                    year_constructed INT,
                    grass BIT,
                    dome BIT
                );
            END
        `;

        const createTeamsTable = `
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'teams')
            BEGIN
                CREATE TABLE teams (
                    id INT,
                    school VARCHAR(255),
                    mascot VARCHAR(255),
                    abbreviation VARCHAR(10),
                    alt_name_1 VARCHAR(255),
                    alt_name_2 VARCHAR(255),
                    alt_name_3 VARCHAR(255),
                    classification VARCHAR(50),
                    conference VARCHAR(255),
                    division VARCHAR(100),
                    color VARCHAR(50),
                    alt_color VARCHAR(50),
                    logo_light VARCHAR(255),
                    logo_dark VARCHAR(255),
                    twitter VARCHAR(255),
                    location_id INT
                );
            END
        `;

        const createCalendarTable = `
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'calendar')
            BEGIN
                CREATE TABLE calendar (
                    season INT,
                    week INT,
                    seasonType VARCHAR(50),
                    firstGameStart DATETIME,
                    lastGameStart DATETIME
                );
            END
        `;

        const createGamesTable = `
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'games')
            BEGIN
                CREATE TABLE games (
                    id INT,
                    season INT,
                    week INT,
                    season_type VARCHAR(50),
                    start_time_tbd BIT,
                    start_date DATETIME,
                    completed BIT,
                    neutral_site BIT,
                    conference_game BIT,
                    attendance INT,
                    venue_id INT,
                    venue VARCHAR(255),
                    home_id INT,
                    home_team VARCHAR(255),
                    home_conference VARCHAR(255),
                    home_division VARCHAR(255),
                    home_points INT,
                    home_post_win_prob FLOAT,
                    home_pregame_elo FLOAT,
                    home_postgame_elo FLOAT,
                    away_id INT,
                    away_team VARCHAR(255),
                    away_conference VARCHAR(255),
                    away_division VARCHAR(255),
                    away_points INT,
                    away_post_win_prob FLOAT,
                    away_pregame_elo FLOAT,
                    away_postgame_elo FLOAT,
                    excitement_index FLOAT
                );
            END
        `;

        await pool.request().query(createConferencesTable);
        await pool.request().query(createLocationsTable);
        await pool.request().query(createTeamsTable);
        await pool.request().query(createCalendarTable);
        await pool.request().query(createGamesTable);

        console.log('Tables created successfully');
    } catch (error) {
        console.error('Error creating tables:', error);
    }
}

async function fetchGameDataAndSave() {
    try {
        const response = await axios.get(apiGamesEndpoint, {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            }
        });
        const data = response.data;

        const insertQuery = `
            INSERT INTO games (
                id, season, week, season_type, start_date, start_time_tbd, 
                completed, neutral_site, conference_game, attendance, 
                venue_id, venue, home_id, home_team, home_conference, 
                home_division, home_points, home_post_win_prob, home_pregame_elo, 
                home_postgame_elo, away_id, away_team, away_conference, away_division, 
                away_points, away_post_win_prob, away_pregame_elo, away_postgame_elo, excitement_index
            ) VALUES (
                @id, @season, @week, @season_type, @start_date, @start_time_tbd, 
                @completed, @neutral_site, @conference_game, @attendance, 
                @venue_id, @venue, @home_id, @home_team, @home_conference, 
                @home_division, @home_points, @home_post_win_prob, @home_pregame_elo, 
                @home_postgame_elo, @away_id, @away_team, @away_conference, @away_division, 
                @away_points, @away_post_win_prob, @away_pregame_elo, @away_postgame_elo, @excitement_index
            );
        `;

        for (const item of data) {
            await pool.request()
                .input('id', sql.Int, item.id)
                .input('season', sql.Int, item.season)
                .input('week', sql.Int, item.week)
                .input('season_type', sql.VarChar, item.season_type)
                .input('start_date', sql.DateTime, item.start_date)
                .input('start_time_tbd', sql.Bit, item.start_time_tbd)
                .input('completed', sql.Bit, item.completed)
                .input('neutral_site', sql.Bit, item.neutral_site)
                .input('conference_game', sql.Bit, item.conference_game)
                .input('attendance', sql.Int, item.attendance)
                .input('venue_id', sql.Int, item.venue_id)
                .input('venue', sql.VarChar, item.venue)
                .input('home_id', sql.Int, item.home_id)
                .input('home_team', sql.VarChar, item.home_team)
                .input('home_conference', sql.VarChar, item.home_conference)
                .input('home_division', sql.VarChar, item.home_division)
                .input('home_points', sql.Int, item.home_points)
                .input('home_post_win_prob', sql.Float, item.home_post_win_prob)
                .input('home_pregame_elo', sql.Float, item.home_pregame_elo)
                .input('home_postgame_elo', sql.Float, item.home_postgame_elo)
                .input('away_id', sql.Int, item.away_id)
                .input('away_team', sql.VarChar, item.away_team)
                .input('away_conference', sql.VarChar, item.away_conference)
                .input('away_division', sql.VarChar, item.away_division)
                .input('away_points', sql.Int, item.away_points)
                .input('away_post_win_prob', sql.Float, item.away_post_win_prob)
                .input('away_pregame_elo', sql.Float, item.away_pregame_elo)
                .input('away_postgame_elo', sql.Float, item.away_postgame_elo)
                .input('excitement_index', sql.Float, item.excitement_index)
                .query(insertQuery);
        }

        console.log('Game data saved');
    } catch (error) {
        console.error('Error saving game data:', error);
    }
}

async function fetchConferenceDataAndSave() {
    try {
        const response = await axios.get(apiConferencesEndpoint, {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            }
        });
        const data = response.data;

        const insertQuery = `
            INSERT INTO conferences (
                id, name, short_name, abbreviation, classification
            ) VALUES (
                @id, @name, @short_name, @abbreviation, @classification
            );
        `;

        for (const item of data) {
            await pool.request()
                .input('id', sql.Int, item.id)
                .input('name', sql.VarChar, item.name)
                .input('short_name', sql.VarChar, item.short_name)
                .input('abbreviation', sql.VarChar, item.abbreviation)
                .input('classification', sql.VarChar, item.classification)
                .query(insertQuery);
        }

        console.log('Conference data saved');
    } catch (error) {
        console.error('Error saving conference data:', error);
    }
}

async function fetchCalendarDataAndSave() {
    try {
        const response = await axios.get(apiCalendarEndpoint, {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            }
        });
        const data = response.data;

        const insertQuery = `
            INSERT INTO calendar (
                season, week, seasonType, firstGameStart, lastGameStart
            ) VALUES (
                @season, @week, @seasonType, @firstGameStart, @lastGameStart
            );
        `;

        for (const item of data) {
            await pool.request()
                .input('season', sql.Int, item.season)
                .input('week', sql.Int, item.week)
                .input('seasonType', sql.VarChar, item.seasonType)
                .input('firstGameStart', sql.DateTime, item.firstGameStart)
                .input('lastGameStart', sql.DateTime, item.lastGameStart)
                .query(insertQuery);
        }

        console.log('Calendar data saved');
    } catch (error) {
        console.error('Error saving calendar data:', error);
    }
}

async function fetchTeamDataAndSave(conf) {
    try {
        const getUrl = apiTeamsEndpoint + conf;
        const response = await axios.get(getUrl, {
            headers: {
                'accept': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            }
        });
        const data = response.data;

        const insertQuery = `
            INSERT INTO teams (
                id, school, mascot, abbreviation, alt_name_1, alt_name_2,
                alt_name_3, classification, conference, division, color, 
                alt_color, logo_light, logo_dark, twitter, location_id
            ) VALUES (
                @id, @school, @mascot, @abbreviation, @alt_name_1, @alt_name_2,
                @alt_name_3, @classification, @conference, @division, @color, 
                @alt_color, @logo_light, @logo_dark, @twitter, @location_id
            );
        `;

        for (const item of data) {
            const location = {
                venue_id: item.location.venue_id,
                name: item.location.name,
                city: item.location.city,
                state: item.location.state,
                zip: item.location.zip,
                country_code: item.location.country_code,
                timezone: item.location.timezone,
                latitude: item.location.latitude,
                longitude: item.location.longitude,
                elevation: item.location.elevation,
                capacity: item.location.capacity,
                year_constructed: item.location.year_constructed,
                grass: item.location.grass,
                dome: item.location.dome
            };

            await saveLocationData(location);

            const logoDark = item.logos[1];
            const logoLight = item.logos[0];

            await pool.request()
                .input('id', sql.Int, item.id)
                .input('school', sql.VarChar, item.school)
                .input('mascot', sql.VarChar, item.mascot)
                .input('abbreviation', sql.VarChar, item.abbreviation)
                .input('alt_name_1', sql.VarChar, item.alt_name_1)
                .input('alt_name_2', sql.VarChar, item.alt_name_2)
                .input('alt_name_3', sql.VarChar, item.alt_name_3)
                .input('classification', sql.VarChar, item.classification)
                .input('conference', sql.VarChar, item.conference)
                .input('division', sql.VarChar, item.division)
                .input('color', sql.VarChar, item.color)
                .input('alt_color', sql.VarChar, item.alt_color)
                .input('logo_light', sql.VarChar, logoLight)
                .input('logo_dark', sql.VarChar, logoDark)
                .input('twitter', sql.VarChar, item.twitter)
                .input('location_id', sql.Int, item.location.venue_id)
                .query(insertQuery);
        }

        console.log('Team data saved for conference:', conf);
    } catch (error) {
        console.error('Error saving team data:', error);
    }
}

async function saveLocationData(location) {
    const insertLocationQuery = `
        INSERT INTO locations (
            venue_id, name, city, state, zip, country_code, timezone, 
            latitude, longitude, elevation, capacity, year_constructed, 
            grass, dome
        ) VALUES (
            @venue_id, @name, @city, @state, @zip, @country_code, @timezone, 
            @latitude, @longitude, @elevation, @capacity, @year_constructed, 
            @grass, @dome
        );
    `;

    try {
        await pool.request()
            .input('venue_id', sql.Int, location.venue_id)
            .input('name', sql.VarChar, location.name)
            .input('city', sql.VarChar, location.city)
            .input('state', sql.VarChar, location.state)
            .input('zip', sql.VarChar, location.zip)
            .input('country_code', sql.VarChar, location.country_code)
            .input('timezone', sql.VarChar, location.timezone)
            .input('latitude', sql.Float, location.latitude)
            .input('longitude', sql.Float, location.longitude)
            .input('elevation', sql.Float, location.elevation)
            .input('capacity', sql.Int, location.capacity)
            .input('year_constructed', sql.Int, location.year_constructed)
            .input('grass', sql.Bit, location.grass)
            .input('dome', sql.Bit, location.dome)
            .query(insertLocationQuery);

        console.log('Location data saved for:', location.name);
    } catch (error) {
        console.error('Error saving location data:', error);
    }
}

async function GetAllOfTheTeams() {
    try {
        const result = await pool.request().query(`SELECT abbreviation FROM conferences WHERE abbreviation IS NOT NULL`);
        const abbreviations = result.recordset.map(row => row.abbreviation);

        for (const team of abbreviations) {
            await fetchTeamDataAndSave(team);
        }
    } catch (error) {
        console.error('Error fetching team abbreviations:', error);
    }
}

async function RunProgram() {
    try {
        // STEP ONE: Open the connection pool
        await openConnection();

        // STEP TWO: Truncate tables (COMMENT THIS OUT IF YOU DON'T HAVE TABLES LOCALLY YET)
        // await truncateTables();

        // STEP THREE: Create the tables
        await createTables();

        // STEP FOUR: Fetch and save Conference Data
        await fetchConferenceDataAndSave();

        // STEP FIVE: Fetch and save Calendar Data
        await fetchCalendarDataAndSave();

        // STEP SIX: Fetch and save Game Data
        await fetchGameDataAndSave();

        // Final STEP: Loop through all of the conferences and get the teams
        await GetAllOfTheTeams();
    } catch (error) {
        console.error('Error running program:', error);
    } finally {
        // Close the connection pool
        await closeConnection();
    }

    const done = `
    |||||||   |||||||||  ||||||   ||||||||
    ||    ||  |||    ||  ||  ||  ||    
    ||    ||  |||    ||  ||  ||  |||||||    
    ||    ||  |||    ||  ||  ||  ||    
    |||||||    |||||||   ||  |||||||||||||
    `;

    console.log(done);
    process.exit(0);
}

RunProgram();
