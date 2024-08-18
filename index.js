const axios = require('axios');
const sql = require('mssql');

const dbConfig = {
    user: 'sa',
    password: 'powerdigitspassword',
    server: 'localhost\\MSSQLSERVER01',
    database: 'PowerDigits',
    options: {
        encrypt: false,
        enableArithAbort: true
    }
};

const year = '2023';
const apiKey = 'EoLeorvh0YUDcFqt7KmceJDbA0J5p8H4VBj/936S4GxdnWOM5d1oPgfL1WJoxbxc';
const apiGamesEndpoint = `https://api.collegefootballdata.com/games?year=${year}&seasonType=regular`;
const apiTeamsEndpoint = `https://api.collegefootballdata.com/teams?conference=`;
const apiConferencesEndpoint = `https://api.collegefootballdata.com/conferences`;
const apiCalendarEndpoint = `https://api.collegefootballdata.com/calendar?year=${year}`;

async function truncateTables() {
    try {
        await sql.connect(dbConfig);
        await sql.query`TRUNCATE TABLE games`;
        await sql.query`TRUNCATE TABLE teams`;
        await sql.query`TRUNCATE TABLE locations`;
        await sql.query`TRUNCATE TABLE conferences`;
        await sql.query`TRUNCATE TABLE calendar`;
        console.log('Tables truncated');
    } catch (error) {
        console.error('Error truncating tables:', error);
    } finally {
        await sql.close();
    }
}

async function createTables() {
    try {
        await sql.connect(dbConfig);

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
                grass BIT,
                dome BIT
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
                firstGameStart DATETIME,
                lastGameStart DATETIME
            );
        `;

        const createGamesTable = `
            CREATE TABLE IF NOT EXISTS games (
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
        `;

        await sql.query(createConferencesTable);
        await sql.query(createLocationsTable);
        await sql.query(createTeamsTable);
        await sql.query(createCalendarTable);
        await sql.query(createGamesTable);

        console.log('Tables created successfully');
    } catch (error) {
        console.error('Error creating tables:', error);
    } finally {
        await sql.close();
    }
}

async function fetchGameDataAndSave() {
    try {
        await sql.connect(dbConfig);

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
            await sql.query(insertQuery, {
                id: item.id,
                season: item.season,
                week: item.week,
                season_type: item.season_type,
                start_date: item.start_date,
                start_time_tbd: item.start_time_tbd,
                completed: item.completed,
                neutral_site: item.neutral_site,
                conference_game: item.conference_game,
                attendance: item.attendance,
                venue_id: item.venue_id,
                venue: item.venue,
                home_id: item.home_id,
                home_team: item.home_team,
                home_conference: item.home_conference,
                home_division: item.home_division,
                home_points: item.home_points,
                home_post_win_prob: item.home_post_win_prob,
                home_pregame_elo: item.home_pregame_elo,
                home_postgame_elo: item.home_postgame_elo,
                away_id: item.away_id,
                away_team: item.away_team,
                away_conference: item.away_conference,
                away_division: item.away_division,
                away_points: item.away_points,
                away_post_win_prob: item.away_post_win_prob,
                away_pregame_elo: item.away_pregame_elo,
                away_postgame_elo: item.away_postgame_elo,
                excitement_index: item.excitement_index
            });
        }

        console.log('Game data saved');
    } catch (error) {
        console.error('Error saving game data:', error);
    } finally {
        await sql.close();
    }
}

async function fetchConferenceDataAndSave() {
    try {
        await sql.connect(dbConfig);

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
            await sql.query(insertQuery, {
                id: item.id,
                name: item.name,
                short_name: item.short_name,
                abbreviation: item.abbreviation,
                classification: item.classification
            });
        }

        console.log('Conference data saved');
    } catch (error) {
        console.error('Error saving conference data:', error);
    } finally {
        await sql.close();
    }
}

async function fetchCalendarDataAndSave() {
    try {
        await sql.connect(dbConfig);

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
            await sql.query(insertQuery, {
                season: item.season,
                week: item.week,
                seasonType: item.seasonType,
                firstGameStart: item.firstGameStart,
                lastGameStart: item.lastGameStart
            });
        }

        console.log('Calendar data saved');
    } catch (error) {
        console.error('Error saving calendar data:', error);
    } finally {
        await sql.close();
    }
}

async function fetchTeamDataAndSave(conf) {
    try {
        await sql.connect(dbConfig);

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

            await sql.query(insertQuery, {
                id: item.id,
                school: item.school,
                mascot: item.mascot,
                abbreviation: item.abbreviation,
                alt_name_1: item.alt_name_1,
                alt_name_2: item.alt_name_2,
                alt_name_3: item.alt_name_3,
                classification: item.classification,
                conference: item.conference,
                division: item.division,
                color: item.color,
                alt_color: item.alt_color,
                logo_light: logoLight,
                logo_dark: logoDark,
                twitter: item.twitter,
                location_id: item.location.venue_id
            });
        }

        console.log('Team data saved for conference:', conf);
    } catch (error) {
        console.error('Error saving team data:', error);
    } finally {
        await sql.close();
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
        await sql.query(insertLocationQuery, {
            venue_id: location.venue_id,
            name: location.name,
            city: location.city,
            state: location.state,
            zip: location.zip,
            country_code: location.country_code,
            timezone: location.timezone,
            latitude: location.latitude,
            longitude: location.longitude,
            elevation: location.elevation,
            capacity: location.capacity,
            year_constructed: location.year_constructed,
            grass: location.grass,
            dome: location.dome
        });
        console.log('Location data saved for:', location.name);
    } catch (error) {
        console.error('Error saving location data:', error);
    }
}

async function GetAllOfTheTeams() {
    try {
        await sql.connect(dbConfig);

        const result = await sql.query`SELECT abbreviation FROM conferences WHERE abbreviation IS NOT NULL`;
        const abbreviations = result.recordset.map(row => row.abbreviation);

        for (const team of abbreviations) {
            await fetchTeamDataAndSave(team);
        }
    } catch (error) {
        console.error('Error fetching team abbreviations:', error);
    } finally {
        await sql.close();
    }
}

async function RunProgram() {
    // STEP ONE: truncate tables (COMMENT THIS OUT IF YOU DON'T HAVE TABLES LOCALLY YET)
    //await truncateTables();

    // STEP TWO: create the tables
    await createTables();

    // STEP THREE: Conference Data
    await fetchConferenceDataAndSave();

    // STEP FOUR: Calendar Data
    await fetchCalendarDataAndSave();

    // STEP FIVE: Game Data
    await fetchGameDataAndSave();

    // Final STEP: Looping through all of the conferences and getting the teams
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
