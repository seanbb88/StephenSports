const axios = require('axios');
const { Client } = require('pg');

const dbConfig = {
    user: 'postgres',
    host: 'localhost',
    database: 'sports',
    password: 'password',
    port: 5432,
};

const year = '2023';
const apiKey = 'EoLeorvh0YUDcFqt7KmceJDbA0J5p8H4VBj/936S4GxdnWOM5d1oPgfL1WJoxbxc';
const apiGamesEndpoint = `https://api.collegefootballdata.com/games?year=${year}&seasonType=regular`;
const apiTeamsEndpoint = `https://api.collegefootballdata.com/teams?conference=`;
const apiConferencesEndpoint = `https://api.collegefootballdata.com/conferences`;
const apiCalendarEndpoint = `https://api.collegefootballdata.com/calendar?year=${year}`;


async function truncateTables() {
    try {
        const client = new Client(dbConfig);
        await client.connect();

        await client.query('TRUNCATE TABLE games');
        await client.query('TRUNCATE TABLE teams');
        await client.query('TRUNCATE TABLE locations');
        await client.query('TRUNCATE TABLE conferences');
        await client.query('TRUNCATE TABLE calendar');
        console.log('Tables truncated');
    } catch (error) {
        console.error('Error truncating tables:', error);
    }
}


async function createTables() {
    try {
        const client = new Client(dbConfig);
        await client.connect();

        const createConferencesTable = `
            CREATE TABLE IF NOT EXISTS conferences (
                id INT,
                name VARCHAR(255),
                short_name VARCHAR(100),
                abbreviation VARCHAR(10),
                classification VARCHAR(50)
            );
        `;

        const createLocationsTable = `
            CREATE TABLE IF NOT EXISTS locations (
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
                grass BOOLEAN,
                dome BOOLEAN
            );
        `;

        const createTeamsTable = `
            CREATE TABLE IF NOT EXISTS teams (
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
        `;

        const createCalendarTable = `
            CREATE TABLE IF NOT EXISTS calendar (
                season INT,
                week INT,
                seasonType VARCHAR(50),
                firstGameStart TIMESTAMP,
                lastGameStart TIMESTAMP
            );
        `;

        const createGamesTable = `
            CREATE TABLE IF NOT EXISTS games (
                id INT,
                season INT,
                week INT,
                season_type VARCHAR(50),
                start_time_tbd BOOLEAN,
                start_date TIMESTAMP,
                completed BOOLEAN,
                neutral_site BOOLEAN,
                conference_game BOOLEAN,
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
        `;

        await client.query(createConferencesTable);
        await client.query(createLocationsTable);
        await client.query(createTeamsTable);
        await client.query(createCalendarTable);
        await client.query(createGamesTable);

        console.log('Tables created successfully');
        await client.end();
    } catch (error) {
        console.error('Error creating tables:', error);
    }
}

async function fetchGameDataAndSave() {
    const client = new Client(dbConfig);
    try {

        await client.connect();

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
            $1, $2, $3, $4, $5, $6, 
            $7, $8, $9, $10, 
            $11, $12, $13, $14, $15, 
            $16, $17, $18, 
            $19, $20, $21, 
            $22, $23, $24, 
            $25, $26, $27, $28, 
            $29
        );
    `;

        for (const item of data) {
            await client.query(insertQuery, [
                item.id,
                item.season,
                item.week,
                item.season_type,
                item.start_date,
                item.start_time_tbd,
                item.completed,
                item.neutral_site,
                item.conference_game,
                item.attendance,
                item.venue_id,
                item.venue,
                item.home_id,
                item.home_team,
                item.home_conference,
                item.home_division,
                item.home_points,
                item.home_post_win_prob,
                item.home_pregame_elo,
                item.home_postgame_elo,
                item.away_id,
                item.away_team,
                item.away_conference,
                item.away_division,
                item.away_points,
                item.away_post_win_prob,
                item.away_pregame_elo,
                item.away_postgame_elo,
                item.excitement_index
            ]);
        }

        console.log('Game data saved');
    } catch (error) {
        console.error('Error saving game data:', error);
    } finally {
        await client.end();
    }
}

async function fetchConferenceDataAndSave() {
    const client = new Client(dbConfig);
    try {

        await client.connect();

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
                $1, $2, $3, $4, $5
            );
        `;

        for (const item of data) {
            await client.query(insertQuery, [
                item.id,
                item.name,
                item.short_name,
                item.abbreviation,
                item.classification
            ]);

        }

        console.log('Conference data saved');
    } catch (error) {
        console.error('Error saving conference data:', error);
    } finally {
        await client.end();
    }
}

async function fetchCalendarDataAndSave() {
    const client = new Client(dbConfig);
    try {

        await client.connect();

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
                $1, $2, $3, $4, $5
            );
        `;

        for (const item of data) {
            await client.query(insertQuery, [
                item.season,
                item.week,
                item.seasonType,
                item.firstGameStart,
                item.lastGameStart
            ]);

        }

        console.log('Calendar data saved');
    } catch (error) {
        console.error('Error saving calendar data:', error);
    } finally {
        await client.end();
    }
}

async function fetchTeamDataAndSave(conf) {
    const client = new Client(dbConfig);
    try {
        const getUrl = apiTeamsEndpoint + conf;
        await client.connect();
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
                $1, $2, $3, $4, $5, $6, 
                $7, $8, $9, $10, 
                $11, $12, $13, $14, $15, $16
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

            await saveLocationData(location, client);

            const logoDark = item.logos[1];
            const logoLight = item.logos[0];

            await client.query(insertQuery, [
                item.id,
                item.school,
                item.mascot,
                item.abbreviation,
                item.alt_name_1,
                item.alt_name_2,
                item.alt_name_3,
                item.classification,
                item.conference,
                item.division,
                item.color,
                item.alt_color,
                logoLight,
                logoDark,
                item.twitter,
                item.location.venue_id
            ]);
        }

        console.log('Team conference team data saved for: ', conf);

    } catch (error) {
        console.error('Error saving team data:', error);
    } finally {
        await client.end();
    }
}

async function saveLocationData(location, client) {
    const insertLocationQuery = `
        INSERT INTO locations (
            venue_id, name, city, state, zip, country_code, timezone, 
            latitude, longitude, elevation, capacity, year_constructed, 
            grass, dome
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        );
    `;

    try {
        await client.query(insertLocationQuery, [
            location.venue_id,
            location.name,
            location.city,
            location.state,
            location.zip,
            location.country_code,
            location.timezone,
            location.latitude,
            location.longitude,
            location.elevation,
            location.capacity,
            location.year_constructed,
            location.grass,
            location.dome
        ]);
        console.log('Location data saved for: ', location.name);
    } catch (error) {
        console.error('Error saving location data:', error);
    }
}


async function GetAllOfTheTeams() {
    const client = new Client(dbConfig);

    try {
        await client.connect();

        const res = await client.query('SELECT abbreviation FROM conferences WHERE abbreviation IS NOT NULL');
        const abbreviations = res.rows.map(row => row.abbreviation);

        for (const team of abbreviations) {
            await fetchTeamDataAndSave(team);
        }

    } catch (error) {
        console.error('Error fetching team abbreviations:', error);
    } finally {
        await client.end();
    }
}


async function RunProgram() {
    //STEP ONE: truncate tables (COMMENT THIS OUT IF U DONT HAVE TABLES LOCALLY YET)
    await truncateTables()

    //STEP TWO: create the tables
    await createTables();

    //STEP THREE: Conference Data 
    await fetchConferenceDataAndSave();

    //STEP FOUR: Calendar Data 
    await fetchCalendarDataAndSave();

    //STEP FIVE: Game Data
    await fetchGameDataAndSave();

    //Final STEP: Looping through all of the conferences and getting the teams
    GetAllOfTheTeams().then(() => {

        const done = `
        |||||||   |||||||||  ||||||   ||||||||
        ||    ||  |||    ||  ||  ||  ||    
        ||    ||  |||    ||  ||  ||  |||||||    
        ||    ||  |||    ||  ||  ||  ||    
        |||||||    |||||||   ||  |||||||||||||
            `;

        console.log(done);
        process.exit(0);
    });

}





RunProgram();
