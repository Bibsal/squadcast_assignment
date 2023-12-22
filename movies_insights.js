const { Client } = require('pg');

const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'squadcast',
    password: 'root',
    port: 5432,
});

client.connect((err) => {
    if (err) {
        console.error('Error connecting to the database:', err.stack);
        return;
    }
    console.log('Connected to the database');

    // a. Top 5 movies based on Duration
    const topDurationQuery = `
    SELECT m.title, m.minutes
    FROM movie m
    ORDER BY m.minutes DESC
    LIMIT 5;
    `;

    client.query(topDurationQuery, (err, res) => {
        if (err) throw err;
        console.log('Top 5 Movies based on Duration:');
        console.table(res.rows);
    });

    // a. Top 5 movies based on Year of Release
    const topReleaseYearQuery = `
    SELECT m.title, m.year
    FROM movie m
    ORDER BY m.year DESC
    LIMIT 5;
    `;

    client.query(topReleaseYearQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop 5 Movies based on Year of Release:');
        console.table(res.rows);
    });

    // a. Top 5 movies based on Average Rating (consider movies with minimum 5 ratings)
    const topAvgRatingQuery = `
    SELECT m.title, AVG(mr.rating) as "Average Rating"
    FROM movie m
    JOIN movie_rating mr ON m.id = mr.movie_id
    WHERE mr.rating >= 5
    GROUP BY m.title
    ORDER BY "Average Rating" DESC
    LIMIT 5;
    `;

    client.query(topAvgRatingQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop 5 Movies based on Average Rating:');
        console.table(res.rows);
    });

    // a. Top 5 movies based on Number of Ratings Given
    const topNumRatingsQuery = `
    SELECT m.title, COUNT(mr.rating) as "Number of Ratings"
    FROM movie m
    JOIN movie_rating mr ON m.id = mr.movie_id
    WHERE mr.rating >= 5
    GROUP BY m.title
    ORDER BY "Number of Ratings" DESC
    LIMIT 5;
    `;

    client.query(topNumRatingsQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop 5 Movies based on Number of Ratings:');
        console.table(res.rows);
    });

    // b. Number of Unique Raters
    const uniqueRatersQuery = `
    SELECT COUNT(DISTINCT rater_id) AS "Unique Raters"
    FROM movie_rating;
    `;

    client.query(uniqueRatersQuery, (err, res) => {
        if (err) throw err;
        console.log('\nNumber of Unique Raters:');
        console.table(res.rows);
    });

    // c. Top 5 Rater IDs based on Most Movies Rated
    const topRatersByMoviesRatedQuery = `
    SELECT rater_id, COUNT(movie_id) as "Movies Rated"
    FROM movie_rating
    GROUP BY rater_id
    ORDER BY "Movies Rated" DESC
    LIMIT 5;
    `;

    client.query(topRatersByMoviesRatedQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop 5 Rater IDs based on Most Movies Rated:');
        console.table(res.rows);
    });

    // c. Top 5 Rater IDs based on Highest Average Rating Given (consider raters with min 5 ratings)
    const topRatersByAvgRatingQuery = `
    SELECT rater_id, AVG(rating) as "Average Rating"
    FROM movie_rating
    GROUP BY rater_id
    ORDER BY "Average Rating" DESC
    LIMIT 5;
    `;

    client.query(topRatersByAvgRatingQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop 5 Rater IDs based on Highest Average Rating Given:');
        console.table(res.rows);
    });

    // d. Top Rated Movie by Director 'Michael Bay'
    const topRatedMovieByDirectorQuery = `
    SELECT title, AVG(rating) as "Average Rating"
    FROM movie m
    JOIN movie_rating r ON m.id = r.movie_id
    WHERE m.director = 'Michael Bay'
    GROUP BY title
    HAVING COUNT(r.rating) >= 5
    ORDER BY "Average Rating" DESC
    LIMIT 1;
    `;

    client.query(topRatedMovieByDirectorQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop Rated Movie by Director \'Michael Bay\':');
        console.table(res.rows);
    });

    // d. Top Rated Comedy Movie
    const topRatedComedyMovieQuery = `
    SELECT title, AVG(rating) as "Average Rating"
    FROM movie m
    JOIN movie_rating r ON m.id = r.movie_id
    WHERE m.genre = 'Comedy'
    GROUP BY title
    HAVING COUNT(r.rating) >= 5
    ORDER BY "Average Rating" DESC
    LIMIT 1;
    `;

    client.query(topRatedComedyMovieQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop Rated Comedy Movie:');
        console.table(res.rows);
    });

    // d. Top Rated Movie in the Year 2013
    const topRatedMovieIn2013Query = `
    SELECT title, AVG(rating) as "Average Rating"
    FROM movie m
    JOIN movie_rating r ON m.id = r.movie_id
    WHERE m.year = 2013
    GROUP BY title
    ORDER BY "Average Rating" DESC
    LIMIT 1;
    `;

    client.query(topRatedMovieIn2013Query, (err, res) => {
        if (err) throw err;
        console.log('\nTop Rated Movie in the Year 2013:');
        console.table(res.rows);
    });

    // d. Top Rated Movie in India
    const topRatedMovieInIndiaQuery = `
        SELECT title, AVG(rating) as "Average Rating"
        FROM movie m
        JOIN movie_rating r ON m.id = r.movie_id
        WHERE m.country = 'India'
        GROUP BY title
        ORDER BY "Average Rating" DESC
        LIMIT 1;
    `;

    client.query(topRatedMovieInIndiaQuery, (err, res) => {
        if (err) throw err;
        console.log('\nTop Rated Movie in India:');
        console.table(res.rows);
    });

    // e. Favorite Movie Genre of Rater ID 1040
    const favoriteGenreOfRater1040Query = `
        SELECT genre, COUNT(*) as "genre_count"
        FROM movie
        JOIN movie_rating ON movie.id = movie_rating.movie_id
        WHERE rater_id = 1040
        GROUP BY genre
        ORDER BY "genre_count" DESC
        LIMIT 1;
    `;

    client.query(favoriteGenreOfRater1040Query, (err, res) => {
        if (err) throw err;
        console.log('\nFavorite Movie Genre of Rater ID 1040:');
        console.table(res.rows);
    });

    // f. Highest Average Rating for a Movie Genre by Rater ID 1040
    const highestAvgRatingByGenreQuery = `
    SELECT genre, AVG(rating) as "Average Rating"
    FROM movie
    JOIN movie_rating ON movie.id = movie_rating.movie_id
    WHERE rater_id = 1040
    GROUP BY genre
    HAVING COUNT(rating) >= 5
    ORDER BY "Average Rating" DESC
    LIMIT 1;
    `;

    client.query(highestAvgRatingByGenreQuery, (err, res) => {
        if (err) throw err;
        console.log('\nHighest Average Rating for a Movie Genre by Rater ID 1040:');
        console.table(res.rows);
    });

    // g. Year with Second-Highest Number of Action Movies
    const secondHighestActionMoviesYearQuery = `
    SELECT year, COUNT(*) AS "Action Movies Count"
    FROM movie
    WHERE genre = 'Action' AND country = 'USA' AND minutes < 120
    GROUP BY year
    ORDER BY "Action Movies Count" DESC, year DESC
    LIMIT 1 OFFSET 1;
    `;

    client.query(secondHighestActionMoviesYearQuery, (err, res) => {
        if (err) throw err;
        console.log('\nYear with Second-Highest Number of Action Movies:');
        console.table(res.rows);
    });

    // h. Count of Movies with High Ratings
    const highRatingsCountQuery = `
    SELECT COUNT(*) AS "High Ratings Count"
    FROM (
        SELECT movie_id
        FROM movie_rating
        WHERE rating >= 7
        GROUP BY movie_id
        HAVING COUNT(*) >= 5
    ) AS high_rated_movies;
    `;

    client.query(highRatingsCountQuery, (err, res) => {
        if (err) throw err;
        console.log('\nCount of Movies with High Ratings:');
        console.table(res.rows);
    });

    // Closing the connection as I've completed all the queries which was given in the assignment.
    client.end();
});
