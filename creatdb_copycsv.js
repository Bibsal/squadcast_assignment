const fs = require('fs');
const pgp = require('pg-promise')();

const connectionString = 'postgres://root:root@localhost:5432/squadcast';

// Creating a PostgreSQL database instance
const db = pgp(connectionString);

// Creating a function to create tables with name movie and movie_rating
async function createTables() {
    try {
        await db.none(`
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                year INTEGER,
                country VARCHAR(255),
                genre VARCHAR(255),
                director VARCHAR(255),
                minutes INTEGER,
                poster VARCHAR(255)
            );
            
            CREATE TABLE IF NOT EXISTS movie_rating (
                movie_id INTEGER REFERENCES movies(id),
                rating FLOAT,
                time TIMESTAMP,
                rater_id INTEGER
            );
        `);
        console.log('Tables created successfully.');
    } catch (error) {
        console.error('Error creating tables:', error);
    }
}

// Created another function to import CSV data
async function importCSVData() {
    try {
        // Importing movie data
        const moviesData = fs.readFileSync('C:\Users\bisha\Downloads\movies_csv\movies_csv', 'utf8');
        await db.none('COPY movies(id, title, year, country, genre, director, minutes, poster) FROM $1 CSV HEADER', [moviesData]);

        // Import movie_rating data which I've stored in a different folder.
        const movieRatingData = fs.readFileSync('C:\Users\bisha\Downloads\movies_csv\movies_rating_csv', 'utf8');
        await db.none('COPY movie_rating(movie_id, rating, time, rater_id) FROM $1 CSV HEADER', [movieRatingData]);

        console.log('CSV data imported successfully.');
    } catch (error) {
        console.error('Error importing CSV data:', error);
    } finally {
        pgp.end();
    }
}

// Running the script
async function main() {
    await createTables();
    await importCSVData();
}

main();
