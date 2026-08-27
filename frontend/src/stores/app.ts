// src/stores/app.ts
import { defineStore } from 'pinia'
import { ref } from 'vue'

/** Client-only UI state. Server data is owned by Vue Query. */
export const useAppStore = defineStore('app', () => {
  const sidebarCollapsed = ref(false)
  const setSidebarCollapsed = (value: boolean) => {
    sidebarCollapsed.value = value
  }
  return { sidebarCollapsed, setSidebarCollapsed }
})
