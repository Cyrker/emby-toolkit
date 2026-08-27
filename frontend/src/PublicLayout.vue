<!-- src/PublicLayout.vue -->
<template>
  <n-layout style="height: 100vh;">
    <n-layout-content style="display: flex; align-items: center; justify-content: center;">
      <router-view />
    </n-layout-content>
  </n-layout>
</template>

<script setup lang="ts">
// @ts-nocheck -- legacy page logic is transport-migrated; page-family typing follows incrementally.
import { NLayout, NLayoutContent } from 'naive-ui';
</script>