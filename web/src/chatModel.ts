export type ChatMessage = {
  id?: string;
  userId?: string;
  username?: string;
  peerId?: string;
  text: string;
  timestamp: number;
};

export function appendChatMessage(current: ChatMessage[], incoming: ChatMessage, limit = 100): ChatMessage[] {
  if (incoming.id && current.some(message => message.id === incoming.id)) return current;
  const next = [...current, incoming];
  return next.slice(-limit);
}

export function mergeChatMessages(current: ChatMessage[], incoming: ChatMessage[], limit = 100): ChatMessage[] {
  const byId = new Map<string, ChatMessage>();
  const anonymous: ChatMessage[] = [];
  for (const message of [...current, ...incoming]) {
    if (message.id) byId.set(message.id, message);
    else anonymous.push(message);
  }
  return [...byId.values(), ...anonymous]
    .sort((a, b) => a.timestamp - b.timestamp)
    .slice(-limit);
}
