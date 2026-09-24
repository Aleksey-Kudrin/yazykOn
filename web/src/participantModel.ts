export type Participant = {
  peerId: string;
  userId?: string;
  role?: string;
};

export function participantFromMeta(
  peerId: string,
  meta?: { userId?: string; role?: string }
): Participant {
  return { peerId, userId: meta?.userId, role: meta?.role };
}

export function upsertParticipant(
  participants: Participant[],
  participant: Participant
): Participant[] {
  const index = participants.findIndex(item => item.peerId === participant.peerId);
  if (index < 0) return [...participants, participant];
  const next = participants.slice();
  next[index] = { ...next[index], ...participant };
  return next;
}

export function removeParticipant(participants: Participant[], peerId: string): Participant[] {
  return participants.filter(item => item.peerId !== peerId);
}

export function participantUserId(participants: Participant[], peerId: string): string | undefined {
  return participants.find(item => item.peerId === peerId)?.userId;
}
