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
const apiEndpoint = `https://api.collegefootballdata.com/games?year=${year}&seasonType=regular`;

async function fetchGameDataAndSave() {
    const client = new Client(dbConfig);
    try {

        await client.connect();

        //uncomment next line if you want to drop the data in your database
        //await truncateGameInfoTable(client);

        const response = await axios.get(apiEndpoint, {
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
                home_division, home_points, home_line_scores, 
                home_post_win_prob, home_pregame_elo, home_postgame_elo, 
                away_id, away_team, away_conference, away_division, 
                away_points, away_line_scores, away_post_win_prob, 
                away_pregame_elo, away_postgame_elo, excitement_index, 
                highlights, notes
            ) VALUES (
                $1, $2, $3, $4, $5, $6, 
                $7, $8, $9, $10, 
                $11, $12, $13, $14, $15, 
                $16, $17, $18, 
                $19, $20, $21, 
                $22, $23, $24, 
                $25, $26, $27, $28, 
                $29, $30, $31, 
                $32, $33
            )`;

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
                JSON.stringify(item.home_line_scores),
                item.home_post_win_prob,
                item.home_pregame_elo,
                item.home_postgame_elo,
                item.away_id,
                item.away_team,
                item.away_conference,
                item.away_division,
                item.away_points,
                JSON.stringify(item.away_line_scores),
                item.away_post_win_prob,
                item.away_pregame_elo,
                item.away_postgame_elo,
                item.excitement_index,
                item.highlights,
                item.notes
            ]);
        }

        console.log('Data saved');
    } catch (error) {
        console.error('Error saving data:', error);
    } finally {
        await client.end();
    }
}

async function truncateGameInfoTable(client) {
    try {
        await client.query('TRUNCATE TABLE games');
        console.log('Table games truncated');
    } catch (error) {
        console.error('Error truncating table:', error);
    }
}

fetchGameDataAndSave();
