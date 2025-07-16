const apiKey = "";
// const url = "https://api.openai.com/v1/engines/davinci/completions";
// // const url = "https://api.openai.com/v1/responses";
// // const url = "https://api.openai.com/v1/chat/completions";
// const api = async(prompt="")=> {
//   await fetch(url, {
//     method: "POST",
//     headers: {
//         "Authorization": `Bearer ${apiKey}`,
//         "Content-Type": "application/json"
//     },
//     body: JSON.stringify({
//         model: "gpt-4",
//         messages: [{ role: "user", content: prompt }]
//     })
// }).then(response => {
//     if (!response.ok) {
//         throw new Error(`HTTP error! status: ${response.status}`);
//     }
//   }).catch(error => {
//     console.error("Error fetching data:", error);
// })
// }

// const aiButton =  document.getElementById("ai-btn").addEventListener("click", async () => {
//   const prompt = document.getElementById("ai-input").value;
//   const response = await api(prompt);
  
//   if (!response.ok) {
//     console.error("Error:", response.statusText);
//     return;
//   }
  
//   const data = await response.json();
//   const result = data.choices[0].message.content;
//   document.getElementById("output").textContent = result;
//   console.log(result);
//   document.getElementById("ai-input").value = "";
// });



// function OpenaiFetchAPI() {
//     console.log("Calling GPT3")
//     var url = "https://api.openai.com/v1/engines/davinci/completions";
//     var bearer = 'Bearer ' + YOUR_TOKEN
//     fetch(url, {
//         method: 'POST',
//         headers: {
//             'Authorization': bearer,
//             'Content-Type': 'application/json'
//         },
//         body: JSON.stringify({
//             "prompt": "Once upon a time",
//             "max_tokens": 5,
//             "temperature": 1,
//             "top_p": 1,
//             "n": 1,
//             "stream": false,
//             "logprobs": null,
//             "stop": "\n"
//         })


//     }).then(response => {
        
//         return response.json()
       
//     }).then(data=>{
//         console.log(data)
//         console.log(typeof data)
//         console.log(Object.keys(data))
//         console.log(data['choices'][0].text)
        
//     })
//         .catch(error => {
//             console.log('Something bad happened ' + error)
//         });

// }

const url = "https://api.openai.com/v1/chat/completions";

const api = async (prompt = "") => {
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4",  // or "gpt-4-turbo" or "gpt-3.5-turbo"
        messages: [{ role: "user", content: prompt }],
        temperature: 0.7
      })
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error fetching data:", error);
    return null;
  }
};

document.getElementById("ai-btn").addEventListener("click", async () => {
  const prompt = document.getElementById("ai-input").value;
  const data = await api(prompt);

  if (!data) {
    document.getElementById("output").textContent = "Error: Could not get response.";
    return;
  }

  const result = data.choices[0].message.content;
  document.getElementById("output").textContent = result;
  console.log(result);
  document.getElementById("ai-input").value = "";
});
