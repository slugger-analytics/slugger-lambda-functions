import pg from 'pg';
const { Pool } = pg;
import dotenv from 'dotenv';

dotenv.config();

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  max: 1
});

export const handler = async () => {
  try {
    const url = `${process.env.POINTSTREAK_BASE}/baseball/season/scoreboard/${process.env.SEASON_ID}/json`;
    console.log("Fetching scoreboard from:", url);

    const resp = await fetch(url, {
      method: "GET",
      headers: {
        "apikey": process.env.POINTSTREAK_API_KEY
      }
    });

    const raw = await resp.text();
    console.log("Downloaded raw JSON, size:", raw.length);

    const data = JSON.parse(raw);
    console.log(data);

    // normalize games array
    const games =
      data.games ??
      (data.schedule?.game ? [data.schedule.game] : []);

    if (games.length === 0) {
      console.log("No games available.");
      return;
    }

    const client = await pool.connect();

    for (const g of games) {
      const pointstreakGameId = g.gameid;

      const homeTeamName = g.hometeam.teamname ?? null;
      const awayTeamName = g.awayteam.teamname ?? null;

      const homeScore = g.homescore ?? null;
      const awayScore = g.awayscore ?? null;

      const gameDate = g.gamedate; // YYYY-MM-DD
      const status = g.gamestatus?.status ?? null;
      const inningsPlayed = g.gamestatus?.inningsplayed ?? null;
      const regulationInnings = g.gamestatus?.regulationinnings ?? null;

      const gameTime = g.gametime ?? null;
      const field = g.field ?? null;
      const timezone = g.timezone ?? null;

      // upsert game record
      await client.query(
        `
        INSERT INTO scores (
          game_id,
          date,
          home_team_name,
          visiting_team_name,
          home_team_score,
          visiting_team_score,
          game_status,
          innings_played,
          regulation_innings,
          gametime,
          field,
          timezone,
          last_updated
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW()
        )
        ON CONFLICT (game_id)
        DO UPDATE SET
          date = EXCLUDED.date,
          home_team_name = EXCLUDED.home_team_name,
          visiting_team_name = EXCLUDED.visiting_team_name,
          home_team_score = EXCLUDED.home_team_score,
          visiting_team_score = EXCLUDED.visiting_team_score,
          game_status = EXCLUDED.game_status,
          innings_played = EXCLUDED.innings_played,
          regulation_innings = EXCLUDED.regulation_innings,
          gametime = EXCLUDED.gametime,
          field = EXCLUDED.field,
          timezone = EXCLUDED.timezone,
          last_updated = NOW()
        `,
        [
          pointstreakGameId,
          gameDate,
          homeTeamName,
          awayTeamName,
          homeScore,
          awayScore,
          status,
          inningsPlayed,
          regulationInnings,
          gameTime,
          field,
          timezone
        ]
      );
    }

    client.release();
    console.log("Scoreboard update complete.");
  } catch (err) {
    console.error("Lambda error:", err);
    throw err;
  }
};

handler({ test: true }).then(console.log).catch(console.error);
