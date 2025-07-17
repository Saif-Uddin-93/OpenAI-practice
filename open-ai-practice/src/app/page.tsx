"use client";
import { useState } from "react";
// const baseURL = "https://api.openai.com/v1/responses";

// const client = new OpenAI(openAIConfig);

// export async function POST(req: NextRequest) {
//   const body = await req.json();
//   const { prompt } = body;

//   const completion = await client.chat.completions.create({
//     model: "gpt-4o",
//     messages: [
//       { role: "user", content: prompt || "Tell me a three sentence bedtime story about a unicorn." }
//     ]
//   });

//   return NextResponse.json(completion);
// }

// const client = new OpenAI(openAIConfig);

export default function Home() {
  const [prompt, setPrompt] = useState("");
  const [clicked, setClicked] = useState(false);
  const [count, setCount] = useState(0);
  return (
    <div className="font-sans grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20">
      <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
        <div id="ai-container">
            <input type="text" name="" id="ai-input" placeholder="Type your query here..." value={prompt} onChange={(e) => setPrompt(e.target.value)} />
            <button id="ai-btn" onClick={async()=>{
              setCount(count + 1);
              setClicked(true);
              console.log(`Button clicked`);
              if(clicked){
                const response = await fetch('/api/chat', {
                  method: 'POST',
                  headers: {
                    'Content-Type': 'application/json',
                  },
                  body: JSON.stringify({ prompt }),
                });

                const data = await response.json();
                const aiOutput = data.choices[0].message.content;
                // const response = await client.chat.completions.create({
                //   model: "gpt-4o",
                //   messages: [
                //     {
                //       role: "user",
                //       content: prompt || "Tell me a three sentence bedtime story about a unicorn."
                //     }
                //   ]
                // });
                // const response = await fetch("https://api.openai.com/v1/chat/completions", {
                //   method: "POST",
                //   headers: {
                //     "Authorization": `Bearer ${apiKey}`,
                //     "Content-Type": "application/json"
                //   },
                //   body: JSON.stringify({
                //     model: "gpt-4o",
                //     messages: [
                //       {
                //         role: "user",
                //         content: prompt || "Tell me a story"
                //       }
                //     ]
                //   })
                // });
                console.log(`Button clicked ${count} times`);
                console.log(aiOutput);
                const outputElement = document.getElementById("ai-output");
                if (outputElement) {
                  outputElement.textContent = aiOutput;
                }
                setClicked(false);
              }
            }}>Submit</button>
            <p id="ai-output">Welcome to the OpenAI test page!</p>
        </div>
      </main>
      <footer className="row-start-3 flex gap-[24px] flex-wrap items-center justify-center">
      </footer>
    </div>
  );
}
