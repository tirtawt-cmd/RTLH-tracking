const sizes = ['300px', '400px', '500px', '600px', '800px', '1000px'];
for (const size of sizes) {
  const url = `https://upload.wikimedia.org/wikipedia/commons/thumb/5/5d/Coat_of_arms_of_West_Kalimantan.svg/${size}-Coat_of_arms_of_West_Kalimantan.svg.png`;
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'SILPBJ-Kalbar/1.2 (TirtaWT@gmail.com; Chrome/120.0.0.0; NodeJs)'
    }
  });
  console.log(`Size ${size}:`, res.status, res.statusText);
  if (res.ok) {
    const buf = await res.arrayBuffer();
    console.log(`-> SUCCESS! Size is ${buf.byteLength} bytes.`);
  }
}
