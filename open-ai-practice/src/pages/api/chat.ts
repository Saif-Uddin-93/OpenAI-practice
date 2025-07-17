import openai from '@/utils/openai';
import { NextApiRequest, NextApiResponse } from 'next';

type Data = {
  name: string;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse<Data>) {
  const completion = await openai.chat.completions.create({
    model: "gpt-3.5-turbo",
    messages: [{ 
      role: "user", 
      content: "Tell me a three sentence bedtime story about a unicorn." 
    }],
    temperature: 0.7,
  });
  res.status(200).json(completion.data.choices[0].message.content);
}