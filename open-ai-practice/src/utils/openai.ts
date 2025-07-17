const { OpenAI } = require('openai');

const openai = new OpenAI({
  baseURL: "https://api.openai.com/v1",
  apiKey: process.env.apiKey,
  maxRetires: 0,
});

export default openai;