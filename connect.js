const http = require('http');
const url = require('url');
const pg = require('pg');

// Create a PostgreSQL pool
const pool = new pg.Pool({
  user: 'postgres',
  password: 'lakshya9685',
  host: 'localhost',
  port: 5432,
  database: 'movieassessment'
});

// Create an HTTP server
const server = http.createServer((req, res) => {
  const { pathname, query } = url.parse(req.url, true);

  if (req.method === 'GET') {
    if (pathname === '/api/v1/longest-duration-movies') { 
      // Logic for GET /api/v1/longest-duration-movies
      getLongestDurationMovies()
      .then(movies => {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(movies));
          console.log('get longest moives is executed');  
        })
        .catch(error => {
          console.error('Error retrieving longest duration movies:', error);
          res.statusCode = 500;
          res.end('Internal Server Error');
        });
    } else if (pathname === '/api/v1/top-rated-movies') {
      // Logic for GET /api/v1/top-rated-movies
      getTopRatedMovies()
        .then(movies => {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(movies));
          console.log('get top rated moives is executed');
        })
        .catch(error => {
          console.error('Error retrieving top rated movies:', error);
          res.statusCode = 500;
          res.end('Internal Server Error');
          console.log('error in top rated');
        });
    } else if (pathname === '/api/v1/genre-movies-with-subtotals') {
      // Logic for GET /api/v1/genre-movies-with-subtotals
      getGenreMoviesWithSubtotals()
        .then(movies => {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(movies));
        })
        .catch(error => {
          console.error('Error retrieving genre movies with subtotals:', error);
          res.statusCode = 500;
          res.end('Internal Server Error');
        });
    } else {
      // Handle unknown routes
      res.statusCode = 404;
      res.end('Not Found');
    }
  } else if (req.method === 'POST') {
    // ...

// Logic for POST /api/v1/new-movie
if (pathname === '/api/v1/new-movie') {
  let body = '';
  req.on('data', chunk => {
    body += chunk.toString();
  });
  req.on('end', () => {
    let movieData;
    try {
      movieData = JSON.parse(body);
    } catch (error) {
      console.error('Error parsing JSON:', error);
      res.statusCode = 400;
      res.end('Invalid JSON');
      return;
    }
  
    saveNewMovie(movieData)
      .then(() => {
        res.end('Success');
      })
      .catch(error => {
        console.error('Error saving new movie:', error);
        res.statusCode = 500;
        res.end('Internal Server Error');
      });
  });
}


     else if (pathname === '/api/v1') {
      console.log('inside if condition');
      // Logic for POST /api/v1/update-runtime-minutes
      updateRuntimeMinutes()
      .then(() => {
        res.end('Success');
      })
    } else {
      // Handle unknown routes
      res.statusCode = 404;
      res.end('Not Found');
    }
  }
});

// Start the server
server.listen(3000, () => {
  console.log('Server is running on http://localhost:3000');
});

// Function to retrieve the top 10 movies with the longest runtime
function getLongestDurationMovies() {
  const query = `
    SELECT tconst, primaryTitle, runtimeMinutes, genres
    FROM movies
    ORDER BY runtimeMinutes DESC
    LIMIT 10;
  `;
  return pool.query(query).then(result => result.rows);
}

// Function to retrieve movies with an average rating greater than 6.0, sorted by average rating
function getTopRatedMovies() {
  const query = `
    SELECT m.tconst, m.primaryTitle, m.genres, AVG(r.averageRating) AS averageRating
    FROM movies m
    JOIN ratings r ON m.tconst = r.tconst
    GROUP BY m.tconst, m.primaryTitle, m.genres
    HAVING AVG(r.averageRating) > 6.0
    ORDER BY AVG(r.averageRating) DESC;
  `;
  return pool.query(query).then(result => result.rows);
}

// Function to retrieve genre movies with subtotals of their numVotes
function getGenreMoviesWithSubtotals() {
  const query = `
    SELECT m.genres, COUNT(*) AS numMovies, SUM(r.numVotes) AS numVotesSubtotal
    FROM movies m
    JOIN ratings r ON m.tconst = r.tconst
    GROUP BY m.genres;
  `;
  return pool.query(query).then(result => result.rows);
}

// Function to save a new movie into the database
function saveNewMovie(movieData) {
  const { tconst,titletype, primaryTitle, runtimeMinutes, genres } = movieData;
  const query = `
    INSERT INTO movies (tconst,titletype, primaryTitle, runtimeMinutes, genres)
    VALUES ($1, $2, $3, $4,$5);
  `;
  const values = [tconst,titletype, primaryTitle, runtimeMinutes, genres];
  return pool.query(query, values);
}

// Function to increment runtimeMinutes of all movies based on their genre
 function updateRuntimeMinutes() {
  console.log("before query");
  const query = `
    UPDATE movies
    SET runtimeMinutes = runtimeMinutes +
      CASE
        WHEN genres = 'D' THEN 15
        WHEN genres = 'A' THEN 30
        ELSE 45
      END;
  `;
  console.log("after query");
  return pool.query(query);

 }