import openai from '@/utils/openai';
import { NextApiRequest, NextApiResponse } from 'next';

type Data = {
  name: string;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse<Data>) {

  const { prompt } = req.body;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ 
      role: "user", 
      content: prompt || "Tell me a three sentence bedtime story about a unicorn." 
    }],
    temperature: 0.7,
  });
  res.status(200).json(completion);
}

// {
//   "id": "chatcmpl-BuHvDNOtu7DJan5Y9KkciemMYFs8v",
//   "object": "chat.completion",
//   "created": 1752754959,
//   "model": "gpt-4o-mini-2024-07-18",
//   "choices": [
//     {
//       "index": 0,
//       "message": {
//         "role": "assistant",
//         "content": "Once upon a time in a shimmering forest, a gentle unicorn named Luna discovered a hidden glade filled with sparkling flowers that glowed under the moonlight. Each night, she would dance among them, spreading joy and magic to all the creatures around her. One evening, as she twirled under the stars, she realized that the true magic was not just in the flowers, but in the friendships she had made along the way.",
//         "refusal": null,
//         "annotations": []
//       },
//       "logprobs": null,
//       "finish_reason": "stop"
//     }
//   ],
//   "usage": {
//     "prompt_tokens": 18,
//     "completion_tokens": 87,
//     "total_tokens": 105,
//     "prompt_tokens_details": {
//       "cached_tokens": 0,
//       "audio_tokens": 0
//     },
//     "completion_tokens_details": {
//       "reasoning_tokens": 0,
//       "audio_tokens": 0,
//       "accepted_prediction_tokens": 0,
//       "rejected_prediction_tokens": 0
//     }
//   },
//   "service_tier": "default",
//   "system_fingerprint": null
// }