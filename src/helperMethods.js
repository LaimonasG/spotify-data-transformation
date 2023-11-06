//filter tracks with no name and shorter than 1 minute
export function filterTrackData(data) {
  const filteredData = data.filter(element => element[1] !== '' && parseInt(element[3]) >= 60000);
  return filteredData;
}

export function filterArtistData(trackData, artistData) {
  const filteringIds = new Set();

  trackData.forEach(track => {
    const artistIds = JSON.parse(track[6].replace(/'/g, '"'));

    filteringIds.add(...artistIds);
  });

  const filteredArtistData = artistData.filter(art => filteringIds.has(art[0]));

  return filteredArtistData;
}

//explode release date into year, month and day columns
export function explodeTrackReleaseDate(trackData) {
  return trackData.map(track => {
    track = [...track];

    // Date column may only have the year value
    if (track[7].length == 10) {
      var year = track[7].substring(0, 4);
      var month = track[7].substring(5, 7);
      var day = track[7].substring(8, 10);

      track.splice(7, 1, year, month, day);
    } else if (track[7].length == 4) {
      track.splice(8, 0, 0, 0);
    }

    return track;
  });
}

//transform danceability values into 'low','medium','high' 
export function transformTrackDanceability(trackData) {
  return trackData.map(track => {
    track = [...track];

    if (parseFloat(track[10]) >= 0 && parseFloat(track[10]) < 0.5) {
      track[10] = "Low"
    } else if (parseFloat(track[10]) >= 0.5 && parseFloat(track[10]) <= 0.6) {
      track[10] = "Medium"
    } else if (parseFloat(track[10]) > 0.6 && parseFloat(track[10]) <= 1) {
      track[10] = "High"
    }

    return track;
  });
}

export function cleanupData(array, fieldNr) {
  array.forEach(el1 => {
    //escape double ""
    //  el1[fieldNr] = el1[fieldNr].replace(/""/g, '""""');

    // //replace single quotes inside string with double qoutes
    // el1[fieldNr] = el1[fieldNr].replace(/(?<=\[|,)'|'(?=\]|,)/g, `''`);

    // //replace "" with "
    // el1[fieldNr] = el1[fieldNr].replace(/(?<=[\w])'(?=[\w])/g, `"'"`);

  });
  return array;
}