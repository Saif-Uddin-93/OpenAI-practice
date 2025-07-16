"use client";
import OpenAI from "openai";
import { useState } from "react";
const baseURL = "https://api.openai.com/v1";
// const baseURL = "https://api.openai.com/v1/responses";

const apiKey = "";

const client = new OpenAI({baseURL, apiKey, dangerouslyAllowBrowser: true, maxRetries: 0});

export default function Home() {
  const [prompt, setPrompt] = useState("");
  
  return (
    <div className="font-sans grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        <div id="ai-container">
            <input type="text" name="" id="ai-input" placeholder="Type your query here..." value={prompt}
  onChange={(e) => setPrompt(e.target.value)} />
            <button id="ai-btn" onClick={async()=>{
              const response = await client.chat.completions.create({
                model: "gpt-4o",
                messages: [
                  {
                    role: "user",
                    content: prompt || "Tell me a three sentence bedtime story about a unicorn."
                  }
                ]
              });
              console.log(response);
            }}>Submit</button>
            <p id="ai-output">Welcome to the OpenAI test page!</p>
        </div>
      </main>
      <footer className="row-start-3 flex gap-[24px] flex-wrap items-center justify-center">
      </footer>
    </div>
  );
}
